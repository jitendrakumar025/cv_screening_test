from fastapi import APIRouter
from src.workers.resume_tasks import process_resume_task

router = APIRouter()

@router.post("/start-resume-analysis")
def start_resume_analysis(payload: dict):
    resume_list = payload.get("resume_list", [])
    parameters = payload.get("parameters", {})

    total = len(resume_list)
    for index, resume_text in enumerate(resume_list, start=1):
        process_resume_task.delay(resume_text, parameters, index, total)

    return {"message": "Resume processing tasks dispatched successfully."}
