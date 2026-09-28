# 🤖🔁👱 ADK Human-in-the-Loop Example

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-ADK-4285F4.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAAhGVYSWZNTQAqAAAACAAFARIAAwAAAAEAAQAAARoABQAAAAEAAABKARsABQAAAAEAAABSASgAAwAAAAEAAgAAh2kABAAAAAEAAABaAAAAAAAAAEgAAAABAAAASAAAAAEAA6ABAAMAAAABAAEAAKACAAQAAAABAAAADqADAAQAAAABAAAADgAAAABOylT5AAAACXBIWXMAAAsTAAALEwEAmpwYAAABWWlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iWE1QIENvcmUgNi4wLjAiPgogICA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5bnRheC1ucyMiPgogICAgICA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIgogICAgICAgICAgICB4bWxuczp0aWZmPSJodHRwOi8vbnMuYWRvYmUuY29tL3RpZmYvMS4wLyI+CiAgICAgICAgIDx0aWZmOk9yaWVudGF0aW9uPjE8L3RpZmY6T3JpZW50YXRpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgoZXuEHAAACRUlEQVQoFXVSS2gTQRj+Z3aziUlF1ISkRqqpVpCaixgkkEOaUGktBopuRQXxKF68pIdCpSsYH4gWxINHT8ZmVUR7MaRND4pUFBFfB7GXqt1iAlGS5rn7O7NpFg/6HeZ/zPfN/PMxAOtQFKSd/L8RFYtDOEmWUVBVovM8fgHDDc+iv+I9VxcgAAhdEtUbP16dTL80uRlZUMdUnXREPBb3/7pDm65g1f3iS9094QRjOwBZWwO09REQ3++kD86qY6DLTAydEWOXy8lYqpzjp/4LB+4fzYVmjiXNPTYyVRRi8IIgRijqk4eua65YqnJLzqDA+wPXijdOALpbzocTaHRFeA+IYliPZaVGCD2cHfdVCJRT6lj7zS1dupoGUhCrsREgVc0Ucm1UQXFBIa3IlVJvU5ceiQTmNyPmddR9DbAZur28Wt1xJi4YjiFq2Eeen7q3FM1HGY2DGQPM1dM3C/5izZ6sGdA9Jzm1YAOc2xzfNj3bNfUJ9t2dhj74DRkQgBlk6vhSy+49b8zBG1wBD69xD4wiQI+Zg/dIHUL97Twq8mi9UaSfo03snSXd8HMlkbitBYbGPxyftHHS99HdW0qDkC4MhOIEFlqoALWEhuGoEVia5UQbLCdoffVdcObSV15v1EpPmfdbkWDbVdazhJQ2KwSlx7gIAfeTtz1EEv3F+MFBLqw7nVNIYNoz//oiG5+Cwj4UIpgGYR58tayQwJzLy8kcODxs53E5HN7AI4fy12WW2Nxhy0e5X+rk7Ia286yBMvtq6/gDb7bjW6TkRnEAAAAASUVORK5CYII=)](https://github.com/google/adk-python)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

This is a simple example of using [Agent Development Kit](https://google.github.io/adk-docs/) (ADK) to create a human-in-the-loop workflow.

In this example we will use a human-in-the-loop workflow to send an expense request for manager approval. The agent submits the request and pauses; when the manager approves or rejects it, a webhook resumes the agent with the decision.

## How it works

The agent uses ADK's `LongRunningFunctionTool`, so nothing blocks while a human decides:

1. The agent calls `request_approval`, which creates the request on the FastAPI server along with a callback (the ADK session and the id of this function call). The tool returns `pending` immediately, the agent tells the user it's waiting, and the run ends.
2. The manager clicks `Approve` or `Reject` in the Streamlit dashboard.
3. The FastAPI server calls ADK's `/run` endpoint (the webhook) with a function response carrying the decision for that same function call id. ADK replaces the `pending` result with the decision and the agent picks up where it left off.

Requests (SQLite) and ADK sessions (in `.adk/`) are persisted, so a pending approval survives restarts of either server.


## Running the Example

We will use a FastAPI server to handle the expense requests and a Streamlit app to display the pending requests as a manager approval dashboard.

### Start the FastAPI server

The FastAPI server provides basic CRUD operations for the expense requests, stores them in SQLite (`approvals.db`), and resumes the agent when a request is decided. It expects ADK on `localhost:8000`; override with the `ADK_URL` environment variable.

```bash
uv run server.py
```

The server is now running on `localhost:9000` and provides a REST API for the expense requests.

### Start the Streamlit app

The Streamlit app will display all pending expense requests and allow the manager to approve or reject them.

```bash
uv run streamlit run client.py
```

The Streamlit app is now running on `localhost:8501` and displays a manager approval dashboard.

![Streamlit App](images/streamlit_app.png)


### Start the ADK agent

```bash
uv run adk web
```

This needs an `OPENAI_API_KEY` in `.env`, since the agent runs `gpt-4o-mini` via LiteLLM. ADK stores sessions in `human_in_the_loop/.adk/` by default, so they survive restarts.

Navigate to `localhost:8000` in your browser to access the ADK web interface.

Go ahead and type something like `Amount 500, reason "team dinner"`.

You should see the agent call the `request_approval` tool, which posts a new expense request to the FastAPI server, and then reply that the request is pending. The run ends there: nothing polls or waits.

To approve or reject the request, head to the Streamlit app at `localhost:8501` in your browser and click the `Approve` or `Reject` button for the given expense ID.

![Pending Expense](images/pending_expense.png)

Hit `Reject` or `Approve`. The FastAPI server immediately resumes the agent through the webhook, and it responds based on the human-in-the-loop decision.

The resumed reply is produced server-side, so the ADK web UI doesn't show it live: refresh the page (or re-select the session) to see it.

![ADK Web Interface](images/agent_workflow.png)
