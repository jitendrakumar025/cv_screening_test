import os
import time
import json
import redis
from celery_app import celery
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from typing_extensions import Dict, Any
import logging

# Load environment variables from .env file
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---Initialize clients once per worker process ---
llm = None
redis_client = None


def get_llm():
    """Initializes the LLM if it hasn't been already."""
    global llm
    if llm is None:
        # print("Initializing LLM for this worker process...")
        llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0.5)
    return llm


def get_redis_client():
    """Initializes the Redis client if it hasn't been already."""
    global redis_client
    if redis_client is None:
        # print("Initializing Redis client for this worker process...")
        redis_client = redis.from_url(REDIS_URL)
    return redis_client


@celery.task
def struct_resume_task(
    resume_id: str, resume_text: str, channel_id: str, batch_id: str
):
    """
    Processes a single resume and publishes the result to a Redis Pub/Sub channel.
    """
    try:
        status_payload = {
            "action": "struct/status",
            "message": f"Processing Resume {resume_id}",
            "resume_id": resume_id,
            "batch_id": batch_id,
        }

        initialized_llm = get_llm()
        r = get_redis_client()

        # logger.info(f"[{channel_id}] Processing: {resume_text[:40]}...")

        r.publish(channel_id, json.dumps(status_payload))

        start_time = time.time()

        PROMPT = f"""
     You are an expert in reading and understanding resume/cv. You will get extracted text of the resume, your work is to read and understand the cv text carefully and response with a JSON data structure with following keys and specifications:
     
    Resume_text={resume_text}



     1. "name": User's name 
     2. "first_name":first name of user
     3. "last_name": last name of user if exist.
     4. "phone": Mostly 10 digit number (without any country code)
     5. "email": Email mentioned in the resume
     6. "country_code": country code in ISO 3166-1 format like for India it is IND and for United States of America it is USA
     7. "socialLinks": example given [{{"github": "https://github.com/user-github"}}, {{"linkedin":"https://www.linkedin.com/in/user-linkedin/"}}, {{"portfolio": "personal website link here for example https://myportfolio.io"}},...] give if they are profile links only. Like github, leetcode, codeforces, hackerrank, twitter, medium. Also do not share repo link in the socialLinks, if no username is there leave empty. If you find username in the resume add it here in form of the link. For example if leetcode_username is in resume share {{"leetcode": "https://leetcode.com/u/leetcode_username"}}
     8. "latest_edu": latest college or university name of the candidate
     9. "latest_deg": degree of the latest college he is in. For example 4th Year | Electrical Engineering
     10. "latest_exp": latest experience of the person where he worked
     11. "location": overall location from the resume for ex. New Delhi | India
     12. "city": city if mentioned
     13. "country": Country (return India if you found 'India' word in resume text)
     14. "state": State/Province
     15. "zipCode": ZIP/Postal Code  
     16. "profiles": Give 2 to 4 work specialities according to work category from the resume. This is example work category you may use if profile fit in the list given: ["Web, Mobile & Software Dev","Blockchain, NFT & Cryptocurrency","AI Apps & Integration","Desktop Application Development","Ecommerce Development","Game Design & Development","Mobile Development","Other - Software Development","Product Management & Scrum","QA Testing","Scripts & Utilities","Web & Mobile Design","Web Development","Data Science & Analytics","A/B Testing","Data Extraction / ETL","Data Mining & Management","Data Visualization","Machine Learning","Quantitative Analysis"],
     17. "skills": A list of skills mentioned in resume text or decide according to  work category
     18. "jobRole": A job role will be decided according to resume like Software Engineer, Data Scientist etc.
     19. "experience":list of JSON objects that contains details of user's experiences with following keys: "company","position","location","startDate","endDate","skills","description". As example= [{{
     "company":"company1",
     "position":"position1"
     "location":"location1",
     "skills":["skill1","skill2",...]
     "startDate":"startdate1 "
     "endDate":"enddate1"
     "description":["point1","point2",...]
     }},...]
     20. "education":list of JSON objects that contains details of user's Educations with following keys:"school","degree","field", "grade","startDate","endDate".
     As Example = [{{
     "school":"school/institution1",
     "degree":"degree1",
     "field":"fieldofstudy1",
     "grade":"grade1",
     "startDate":"startdate1"
     "endDate":"enddate1"
     }},...]
     21."projects":list of JSON objects that contains details of user's projects with following keys: "title","description","startDate","endDate","skills","role","url". As Example = [{{
        "title":"project1",
        "description":["point1","point2",...],
        "startDate":"startdate1",
        "role":"role1 (if any)"
        "skills": ["skill1","skill2",...]
        "endDate":"enddate1",
        "url":"url1"
        }},...]
     22. "bio": This will be like TL;DR. Using the resume, generate a concise bio summarizing the overall profile.
     23. "others": This is list of json of other fields that are not listed above for example other fields could be: "publications","patents","thesis","volunteer experience", "honors & awards","license or certification","Extra Curricular Activities" etc.
         -Output example : {{
            "Other Field1":[
                {{"title":"field title1","description":["point1","point2",...],"startDate":"startdate1","endDate":"enddate1","organization":"organization1","location":"location1"}},
                 ...
                ],
            "Other Field2":[
                {{"title":"field title1","description":["point1","point2",...],"startDate":"startdate1","endDate":"enddate1","organization":"organization1","location":"location1"}},
                 ...
                ],
            }}
            
     ** Finally check the whole CV data and JSON Structure, if there is anything MISSING, ADD it.
     ** ALWAYS return all type of Dates in following format= "{{3 letter word for month}} {{4 digit for year}}"
     ** ALWAYS return all the links will always be complete. Like for github.com/username it will be https://github.com/username
     The Final Output Should start with '```json' and trailing with '```'.
""".strip()

        results = initialized_llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )

        end_time = time.time()
        processing_time = round(end_time - start_time, 2)
        result_payload = {
            "action": "struct/result",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "resume_result": json_result,
            "processing_time": processing_time,
        }
        # Publish the result to the unique channel for this job
        r.publish(channel_id, json.dumps(result_payload))
        # logger.info(f"[{channel_id}] Published result for: {resume_text[:40]}...")

    except Exception as e:
        logger.error(f"[{channel_id}] Error processing task: {e}")
        r = get_redis_client()
        error_message = {
            "status": "ERROR",
            "data": {"error": str(e), "original_text": resume_text},
            "resume_id": resume_id,
            "batch_id": batch_id,
        }
        # Publish the error message to the channel
        r.publish(channel_id, json.dumps(error_message))

    return f"Published structured result to channel {channel_id}"


@celery.task
def analyze_resume_task(
    resume_id: str,
    resume_text: str,
    channel_id: str,
    batch_id: str,
    candidate_id: str,
    round_id: str,
    parameters: Dict[str, Any],
):
    """_summary_

    Args:
        resume_id (str): _description_
        resume_text (str): _description_
        channel_id (str): _description_
        batch_id (str): _description_
        candidate_id (str): _description_
        round_id (str): _description_
    """

    try:
        status_payload = {
            "action": "analysis/status",
            "message": f"Processing Resume {resume_id}",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "round_id": round_id,
        }

        initialized_llm = get_llm()
        r = get_redis_client()

        # logger.info(f"[{channel_id}] Processing: {resume_text[:40]}...")

        r.publish(channel_id, json.dumps(status_payload))

        start_time = time.time()

        PROMPT = f"""
            RESUME = {resume_text}

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

        results = initialized_llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )

        end_time = time.time()
        processing_time = round(end_time - start_time, 2)
        result_payload = {
            "action": "analysis/result",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "resume_result": json_result,
            "processing_time": processing_time,
            "candidate_id": candidate_id,
            "round_id": round_id,
        }
        # Publish the result to the unique channel for this job
        r.publish(channel_id, json.dumps(result_payload))
        # logger.info(f"[{channel_id}] Published result for: {resume_text[:40]}...")

    except Exception as e:
        logger.error(f"[{channel_id}] Error processing task: {e}")
        r = get_redis_client()
        error_message = {
            "status": "ERROR",
            "data": {"error": str(e), "original_text": resume_text},
            "candidate_id": candidate_id,
            "round_id": round_id,
        }
        # Publish the error message to the channel
        r.publish(channel_id, json.dumps(error_message))

    return f"Published result to channel {channel_id}"


@celery.task
def pool_analysis_task(
    candidate_profile, parameters, profile_id, batch_id, channel_id: str
):
    """
    This task is used to analyse candidates resumes in talent pool

    Args:
        candidate_profile (_type_): _description_
        parameters (_type_): _description_
        profile_id (_type_): _description_
        batch_id (_type_): _description_
        total_count (_type_): _description_
    """

    try:
        status_payload = {
            "action": "pool-analysis/status",
            "message": f"Processing profile {profile_id}",
            "profile_id": profile_id,
            "batch_id": batch_id,
        }

        initialized_llm = get_llm()
        r = get_redis_client()

        # logger.info(f"[{channel_id}] Processing: {resume_text[:40]}...")

        r.publish(channel_id, json.dumps(status_payload))

        start_time = time.time()

        PROMPT = f"""
        Role: You are a highly experienced AI interview evaluator. Your job is to analyze candidate interview data and career profiles, then objectively evaluate them based on provided evaluation parameters.

        ## You will receive:
        - The candidate's detailed interview analysis and career profile
        - A set of evaluation parameters. Each parameter includes:
        - 'parameter': Name of the evaluation criteria
        - 'description': Explanation of what this criteria assesses
        - 'weightage': Numeric weight reflecting the maximum possible score for this parameter (from 1 to 10, where 10 is most important)

        ## Your tasks:
        1. Carefully review the candidate's interview analysis and career profile.
        2. For each evaluation parameter:
        - Assess how well the candidate meets the expectations based on the description and available data.
        - Assign a score from 0 to the parameter's defined weightage.
            - A score of 0 indicates poor performance or missing data.
            - A score close to the weightage indicates excellent performance.
        - Provide a clear, specific justification for the assigned score using evidence from the interview analysis and career profile.
        - If relevant information for a parameter is missing, clearly mention that in the justification and assign a proportionally reasonable score.
        3. Write a brief summary that assesses **how well the candidate fits for the target job role**, based on the overall performance across the evaluation parameters. 
        - Factor in both the parameter scores and their weightages.
        - Focus on job fitment — not on listing strengths and weaknesses.
        - Clearly state whether the candidate meets, exceeds, or falls short of expectations for the role, especially considering the most important (high weightage) parameters.

        **Important Instructions:**
        - Be objective and base your evaluation only on the provided data.
        - Use professional, neutral, and insightful language in your justifications.
        - Higher weightage parameters should influence the final fitment summary more strongly.
        - Only evaluate what's available — if a parameter is missing data, mention it and adjust the score fairly.
        - Do not make assumptions beyond the available information.

        ## Candidate's Interview Analysis and Career Profile:

        {candidate_profile}

        ## Evaluation Parameters:

        {parameters}

        ---

        ## Response Format:
        Return your evaluation strictly in the following JSON structure:
        ```json
        {{
        "analysis_score": {{
            "summary": "A brief summary explaining how the candidate fits for the target job role, based on the rubric evaluation and considering the parameter weightages.",
            "rubrics": [
            {{
                "name": "Name of the Criteria as provided in the evaluation parameters",
                "score": 0, // Integer (0 - parameter's weightage)
                "justification": "Detailed reasoning explaining how this score was determined based on the provided information."
            }},
            ... // repeat for each parameter
            ]
        }}
        }}
        ```

        Only return valid JSON — no additional commentary or explanation.
"""

        results = initialized_llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )

        end_time = time.time()
        processing_time = round(end_time - start_time, 2)
        result_payload = {
            "action": "pool-analysis/result",
            "profile_id": profile_id,
            "batch_id": batch_id,
            "pool_result": json_result,
            "processing_time": processing_time,
        }
        # Publish the result to the unique channel for this job
        r.publish(channel_id, json.dumps(result_payload))

    except Exception as e:
        logger.error(f"[{channel_id}] Error processing task: {e}")
        r = get_redis_client()
        error_message = {
            "status": "ERROR",
            "data": {"error": str(e), "original_text": candidate_profile},
            "profile_id": profile_id,
        }
        # Publish the error message to the channel
        r.publish(channel_id, json.dumps(error_message))
