from src.llm_utils.initialize_llm import initialize_llm
from src.prompts.prompts import AnalysisPrompt
from typing import Dict, Any, List
import json
import asyncio
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage


################## ----------################
def evaluate_resume_sync(resume: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    try:
        # print("debug in evaluate_resume 1234")
        llm = initialize_llm(1)
        PROMPT = f"""
            RESUME = {resume}

            PARAMETERS = {parameters}
            
            Please evaluate the following resume against the provided evaluation criteria. For each criterion, provide a score from 0-10 (0 being no evidence, 10 being exceptional evidence) and a 20-30 words for reason for the score.

            ### Special Case:
            **PARAMETERS contain a field name "Additional Parameters" is array of json with keys `title`: "additional parameter defined by recruiter", `options`:array that contains option for your response,"type": "your response type on this parameter"

            *For "Additional Parameters" your output should be json with keys `title` and `response`  
            
            ## Output
            STRICTLY ADHERE TO THE FOLLOWING OUTPUT STRUCTURE:
            The output must start and trail with triple backtics.

            ```json
                {{"resume_score": {{
                "summary": "A brief summary of the resume evaluation, highlighting key strengths and weaknesses.",
                "rubrics": [
                    {{ "name": "Name of the Criteria as provided in the evaluation criteria",
                    "score": 4, // Integer (0-10) based on resume match
                    "justification": "Resume mentions relevant experience with [tech/skill], but lacks [specific detail]."
                    }},
                    ... so on
                 ],
                 
                "additional":[{{"title":"additional parameter defined by recruiter",
                  "type":"This will be the same from the additional question given in rubrics additional paramter",
                  "response": ["based on the question if answer requires multiple answers you will answer here in array of strings (ALWAYS). Like for type MULTI_SELECT or TAG."]
                  "answer":"You will answer here when the type is TEXT/TRUE_FALSE or any question which requires explanation. This will always be string not array of strings"
                  }}.
                  ... so on for other additional fields
                  ]
                }}
                 }}```
            
        """

        results = llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )
        # print("Result in evaluaterebadnn>> ",results.content[1:10])
        return json_result
    except Exception as E:
        raise ValueError(f"Error when evaluating resume, Reason: {E}")


# async def evaluate_resumes_in_parallel(
#     resume_list: List[str], parameters: Dict[str, Any]
# ) -> List[Any]:
#     semaphore = asyncio.Semaphore(10)

#     async def process_task(resume: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
#         async with semaphore:
#             eval_result=await evaluate_resume(resume, parameters)
#             print("check1231, ",eval_result)
#             struct_profile=await getCandidateDetails(resume)
#             print("check 2313432",struct_profile)
#             return {eval_result,struct_profile}

#     tasks = [
#         asyncio.create_task(process_task(resume, parameters)) for resume in resume_list
#     ]
#     results = await asyncio.gather(*tasks)
#     print("ficnscahs__>> ",results)
#     return results



