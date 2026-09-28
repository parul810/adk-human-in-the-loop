# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import httpx

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import LongRunningFunctionTool, ToolContext

APPROVAL_SERVER_URL = os.getenv("APPROVAL_SERVER_URL", "http://localhost:9000")

async def request_approval(tool_context: ToolContext, amount: float, reason: str) -> dict:
    """Submit an expense for human approval.

    This is a long-running operation: it returns immediately with status
    "pending". The final decision ("approved" or "rejected") arrives later as
    an updated response to this same call, once a human has reviewed it.

    Args:
        tool_context (ToolContext): The tool context
        amount (float): The amount to approve
        reason (str): The reason for the expense

    Returns:
        dict: The approval request id and its current status
    """
    # Tell the approval server where to deliver the decision: the ADK session
    # and the id of this exact function call, so the webhook can resume it.
    session = tool_context.session
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{APPROVAL_SERVER_URL}/request",
            json={
                "amount": amount,
                "reason": reason,
                "callback": {
                    "app_name": session.app_name,
                    "user_id": session.user_id,
                    "session_id": session.id,
                    "function_call_id": tool_context.function_call_id,
                    "function_name": "request_approval",
                },
            },
        )
        # Error if not 200
        response.raise_for_status()
        request_data = response.json()
    tool_context.state["approval_request_id"] = request_data["id"]
    return {
        "status": "pending",
        "approval_request_id": request_data["id"],
        "amount": amount,
        "reason": reason,
    }

approval_tool = LongRunningFunctionTool(func=request_approval)

root_agent = LlmAgent(
    model=LiteLlm(model="openai/gpt-4o-mini"),
    name="ExpenseApprovalAgent",
    instruction="""You help employees get expenses approved by a human manager.

When the user asks for approval, extract the amount and reason and call
request_approval. If either is missing, ask the user for it first. Never call
request_approval again for an expense that already has a result.

Reply based on the "status" field of the request_approval result:
- "pending": the request is waiting for a manager. Tell the user it was
  submitted, include the approval_request_id, and say you will follow up
  once a manager decides.
- "approved": a manager approved it. Tell the user the expense is approved
  and they can go ahead.
- "rejected": a manager rejected it. Tell the user the expense was rejected
  and they should not proceed.""",
    tools=[approval_tool],
)
