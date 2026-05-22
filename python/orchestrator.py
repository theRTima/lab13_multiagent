import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime

import nats
from nats.errors import TimeoutError as NatsTimeoutError

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class Bid:
    agent_role: str
    cost: float
    estimated_time_ms: int
    current_load: int
    capacity: int
    trace_id: str
    timestamp: str


@dataclass
class PipelineStage:
    name: str
    auction_subject: str
    task_subject: str
    auction_timeout_ms: int = 500
    max_retries: int = 3


class CreditScoringOrchestrator:
    def __init__(self, nats_url: str = "nats://localhost:4222"):
        self.nats_url = nats_url
        self.nc = None
        self.pipeline_stages = [
            PipelineStage(
                name="Data Collection",
                auction_subject="auction.data_collection",
                task_subject="data.collection",
            ),
            PipelineStage(
                name="Income Analysis",
                auction_subject="auction.income_eval",
                task_subject="income.analysis",
            ),
            PipelineStage(
                name="Risk Evaluation",
                auction_subject="auction.risk_evaluation",
                task_subject="risk.evaluation",
            ),
        ]

    async def connect(self):
        """Connect to NATS server."""
        self.nc = await nats.connect(self.nats_url)
        logger.info(f"Connected to NATS: {self.nats_url}")

    async def close(self):
        """Close NATS connection."""
        if self.nc:
            await self.nc.close()

    async def run_auction(self, stage: PipelineStage) -> Optional[Bid]:
        """
        Broadcast auction and collect bids from agents.
        Returns the lowest cost bid.
        """
        trace_id = f"trace-{uuid.uuid4().hex[:8]}"
        auction_request = {
            "task_type": stage.name.lower().replace(" ", "_"),
            "trace_id": trace_id,
        }

        logger.info(f"[{stage.name}] Starting auction on {stage.auction_subject}")

        bids = []
        received_event = asyncio.Event()

        async def bid_handler(msg):
            try:
                bid_data = json.loads(msg.data)
                bid = Bid(**bid_data)
                bids.append(bid)
                logger.info(
                    f"[{stage.name}] Received bid from {bid.agent_role}: "
                    f"cost={bid.cost:.2f}, load={bid.current_load}/{bid.capacity}"
                )
                received_event.set()
            except Exception as e:
                logger.error(f"Failed to parse bid: {e}")

        # Subscribe to bids (bids come back on same reply-to subject)
        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox)

        async def read_bids():
            try:
                async for msg in sub.unsubscribe(after=10):  # Max 10 bids
                    await bid_handler(msg)
            except Exception:
                pass

        # Start reading bids in background
        read_task = asyncio.create_task(read_bids())

        # Broadcast auction
        try:
            await self.nc.publish(
                stage.auction_subject,
                json.dumps(auction_request).encode(),
                reply_to=inbox,
            )
        except Exception as e:
            logger.error(f"Failed to publish auction: {e}")
            read_task.cancel()
            return None

        # Wait for bids with timeout
        try:
            await asyncio.wait_for(
                asyncio.sleep(stage.auction_timeout_ms / 1000),
                timeout=stage.auction_timeout_ms / 1000 + 0.1,
            )
        except asyncio.TimeoutError:
            pass
        finally:
            read_task.cancel()
            await sub.unsubscribe()

        if not bids:
            logger.warning(f"[{stage.name}] No bids received")
            return None

        # Select lowest cost bid
        lowest_bid = min(bids, key=lambda b: b.cost)
        logger.info(
            f"[{stage.name}] Selected agent: {lowest_bid.agent_role} "
            f"(cost: {lowest_bid.cost:.2f})"
        )
        return lowest_bid

    async def execute_stage(
        self, stage: PipelineStage, applicant_data: Dict[str, Any], retry: int = 0
    ) -> Dict[str, Any]:
        """
        Execute a pipeline stage: auction -> bid selection -> task execution.
        """
        logger.info(
            f"\n{'='*60}\n[{stage.name}] Executing stage (attempt {retry + 1})"
        )

        # Run auction
        bid = await self.run_auction(stage)
        if not bid:
            if retry < stage.max_retries - 1:
                logger.warning(f"[{stage.name}] Retrying... ({retry + 1}/{stage.max_retries - 1})")
                await asyncio.sleep(0.1)
                return await self.execute_stage(stage, applicant_data, retry + 1)
            else:
                raise Exception(f"[{stage.name}] Failed after {stage.max_retries} retries")

        # Prepare task
        trace_id = f"trace-{uuid.uuid4().hex[:8]}"
        task = {
            "type": f"{stage.name.lower().replace(' ', '_')}.process",
            "data": applicant_data,
            "trace_id": trace_id,
            "timestamp": datetime.utcnow().isoformat(),
        }

        logger.info(
            f"[{stage.name}] Sending task to {bid.agent_role} on {stage.task_subject}"
        )

        # Send task and wait for response
        try:
            response_msg = await self.nc.request(
                stage.task_subject,
                json.dumps(task).encode(),
                timeout=5,
            )
            result = json.loads(response_msg.data)
            logger.info(f"[{stage.name}] Task completed: {result.get('status', 'done')}")
            return result
        except NatsTimeoutError:
            logger.error(f"[{stage.name}] Task timeout on {bid.agent_role}")
            if retry < stage.max_retries - 1:
                logger.info(f"[{stage.name}] Retrying... ({retry + 1}/{stage.max_retries - 1})")
                await asyncio.sleep(0.1)
                return await self.execute_stage(stage, applicant_data, retry + 1)
            else:
                raise Exception(f"[{stage.name}] Failed after {stage.max_retries} retries")

    async def process_application(self, applicant_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an applicant through the entire credit scoring pipeline.
        """
        applicant_id = applicant_data.get("applicant_id", "unknown")
        logger.info(f"\n{'#'*60}\nProcessing application: {applicant_id}\n{'#'*60}")

        results = {"applicant_id": applicant_id, "stages": {}}
        current_data = applicant_data.copy()

        try:
            for stage in self.pipeline_stages:
                stage_result = await self.execute_stage(stage, current_data)
                results["stages"][stage.name] = stage_result

                # Merge results for next stage
                if isinstance(stage_result.get("result"), dict):
                    current_data.update(stage_result["result"])

            results["status"] = "completed"
            logger.info(f"\n✓ Application {applicant_id} processing completed")

        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
            logger.error(f"\n✗ Application {applicant_id} processing failed: {e}")

        return results


async def main():
    """Demo: Process sample applications through the pipeline."""
    orchestrator = CreditScoringOrchestrator()

    try:
        await orchestrator.connect()

        # Sample applicants
        applicants = [
            {
                "applicant_id": "APP001",
                "name": "John Doe",
                "annual_income": 85000,
                "employment_status": "employed",
                "documents": ["w2", "paystub"],
            },
            {
                "applicant_id": "APP002",
                "name": "Jane Smith",
                "annual_income": 120000,
                "employment_status": "self-employed",
                "documents": ["tax_return", "bank_statements"],
            },
        ]

        # Process applications
        for applicant in applicants:
            result = await orchestrator.process_application(applicant)
            print(f"\nFinal Result:\n{json.dumps(result, indent=2)}")
            await asyncio.sleep(1)

    finally:
        await orchestrator.close()


if __name__ == "__main__":
    asyncio.run(main())
