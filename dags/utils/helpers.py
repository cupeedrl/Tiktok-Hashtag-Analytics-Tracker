"""
Utility Helper Functions
Common utilities used across DAG tasks.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def log_task_start(task_name: str, execution_date: str) -> None:
    """Log task start message"""
    logger.info(f"{'='*60}")
    logger.info(f"Starting task: {task_name}")
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"{'='*60}")


def log_task_end(task_name: str, result: Any) -> None:
    """Log task completion message"""
    logger.info(f"{'='*60}")
    logger.info(f"Completed task: {task_name}")
    logger.info(f"Result: {result}")
    logger.info(f"{'='*60}")


def format_error_message(error: Exception, context: Dict) -> str:
    """Format error message for logging/alerting"""
    return f"""
    ╔══════════════════════════════════════════════════════════╗
    ║                    DAG FAILURE ALERT                     ║
    ╠══════════════════════════════════════════════════════════╣
    ║ DAG ID:       {context.get('dag').dag_id}                ║ 
    ║ Task ID:      {context.get('task').task_id}              ║ 
    ║ Execution:    {context.get('execution_date')}            ║ 
    ║ Error:        {str(error)}                               ║ 
    ╚══════════════════════════════════════════════════════════╝
    """