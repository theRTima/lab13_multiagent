from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import nats
import json
import asyncio
from typing import Dict, List, Any
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
agents_status = {}
task_results = []
queue_status = {}
nc = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global nc
    # Startup
    try:
        nc = await nats.connect("nats://localhost:4222")
        logger.info("Connected to NATS")
        await subscribe_to_agent_updates()
    except Exception as e:
        logger.error(f"Failed to connect to NATS: {e}")
    
    yield
    
    # Shutdown
    if nc:
        await nc.close()


app = FastAPI(title="Agent Monitoring Panel", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


async def subscribe_to_agent_updates():
    """Subscribe to agent status updates"""
    async def msg_handler(msg):
        try:
            data = json.loads(msg.data)
            agent_role = data.get("agent_role", "unknown")
            agents_status[agent_role] = {
                "role": agent_role,
                "queue_length": data.get("queue_length", 0),
                "status": data.get("status", "active"),
                "last_seen": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error processing agent update: {e}")
    
    try:
        await nc.subscribe("agent.status.>", cb=msg_handler)
        logger.info("Subscribed to agent status updates")
    except Exception as e:
        logger.error(f"Failed to subscribe to agent status: {e}")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    context = {
        "request": request,
        "agents": agents_status,
        "queues": queue_status,
        "tasks": task_results
    }
    return templates.TemplateResponse(request, "dashboard.html", context)


@app.get("/api/agents")
async def get_agents():
    """Get all agents status"""
    return JSONResponse(agents_status)


@app.get("/api/queues")
async def get_queues():
    """Get queue status"""
    return JSONResponse(queue_status)


@app.get("/api/tasks")
async def get_tasks():
    """Get task results"""
    return JSONResponse(task_results)


@app.post("/api/launch-task")
async def launch_task(
    task_type: str = Form(...),
    task_data: str = Form(...)
):
    """Launch a new task"""
    try:
        data = json.loads(task_data)
        task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        task = {
            "id": task_id,
            "type": task_type,
            "data": data,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        
        # Publish task to NATS
        subject = f"{task_type.replace('_', '.')}"
        await nc.publish(subject, json.dumps(data).encode())
        
        task["status"] = "launched"
        task_results.append(task)
        
        return JSONResponse({"status": "success", "task_id": task_id})
    except Exception as e:
        logger.error(f"Failed to launch task: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "nats_connected": nc is not None,
        "agents_count": len(agents_status)
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
