import requests
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class RiskAnalyzer:
    """Risk analysis using local Ollama with Qwen3.5 model"""
    
    def __init__(self, ollama_url: str = "http://localhost:11434", model: str = "qwen3.5:latest"):
        self.ollama_url = ollama_url
        self.model = model
        self.api_endpoint = f"{ollama_url}/api/generate"
    
    def analyze_risk(self, text: str) -> Dict[str, Any]:
        """
        Analyze text for risk using Ollama
        
        Args:
            text: Input text to analyze
            
        Returns:
            Dict with risk_level (HIGH/MEDIUM/LOW) and reasoning
        """
        prompt = f"""You are a risk analysis system. Analyze the following text and determine the risk level.
Respond with ONLY one of these three words: HIGH, MEDIUM, or LOW.

Text to analyze:
{text}

Risk level:"""
        
        try:
            response = requests.post(
                self.api_endpoint,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,  # Low temperature for consistent results
                        "num_predict": 10   # Only need a few tokens
                    }
                },
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Ollama API error: {response.status_code} - {response.text}")
                return {"risk_level": "MEDIUM", "reasoning": "API error, defaulting to MEDIUM"}
            
            result = response.json()
            risk_text = result.get("response", "").strip().upper()
            
            # Normalize the risk level
            if "HIGH" in risk_text:
                risk_level = "HIGH"
            elif "LOW" in risk_text:
                risk_level = "LOW"
            else:
                risk_level = "MEDIUM"  # Default
            
            logger.info(f"Risk analysis result: {risk_level}")
            return {
                "risk_level": risk_level,
                "reasoning": f"Analyzed by {self.model}"
            }
            
        except requests.exceptions.Timeout:
            logger.error("Ollama request timed out")
            return {"risk_level": "MEDIUM", "reasoning": "Timeout, defaulting to MEDIUM"}
        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to Ollama. Is it running?")
            return {"risk_level": "MEDIUM", "reasoning": "Connection error, defaulting to MEDIUM"}
        except Exception as e:
            logger.error(f"Risk analysis error: {e}")
            return {"risk_level": "MEDIUM", "reasoning": f"Error: {str(e)}"}
    
    def check_ollama_available(self) -> bool:
        """Check if Ollama is available"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False


def main():
    """Test the risk analyzer"""
    analyzer = RiskAnalyzer()
    
    if not analyzer.check_ollama_available():
        print("ERROR: Ollama is not running. Start it with: ollama serve")
        return
    
    test_cases = [
        "This is a normal credit application with standard income and employment.",
        "The applicant has multiple fraud flags and suspicious transaction patterns.",
        "Application shows some inconsistencies but nothing alarming."
    ]
    
    for text in test_cases:
        print(f"\nAnalyzing: {text[:50]}...")
        result = analyzer.analyze_risk(text)
        print(f"Risk Level: {result['risk_level']}")
        print(f"Reasoning: {result['reasoning']}")


if __name__ == "__main__":
    main()
