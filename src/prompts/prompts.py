from dataclasses import dataclass


@dataclass
class RubricsPrompt():
    GENERATE_RUBRIC_EVALUATION = """
    You are an expert in hiring and resume evaluation. 
    Each evaluation criterion should have a name, description, and an importance score on a scale of 1-5. 

    ## Instructions
    - Carefully analyze the job description to identify hard skills, soft skills, experience, and education requirements. Categorize them into:
        Technical skills (e.g., LLm, JavaScript, API Integration)
        Soft skills (e.g., communication, leadership)
        Experience level (e.g., years of experience, project work)
        Education background (e.g., degree requirements)
        Certifications (if applicable)
    - JOB TITLE: Extract JOB TITLE from job description.
    - Create a list of essential parameter that will be used to assess resumes. These should be aligned with the job description.
    - Each criterion should be rated on a 1-5 scale, where 5 is the most critical and 1 is least important based on the job requirements.
    - Provide Exact tech Stack name in parameter, example - if javascript is mentioned than provide javascript as parameter. 
    - Be very precise with what is needed.
    
    ## Output
    STRICTLY ADHERE TO THE FOLLOWING OUTPUT STRUCTURE:
    The output must start and trail with triple backtics.

    ```json{{
        "job_title": "JOB TITLE here",
        "evaluation_criteria": [
            {{
            "parameter": "Name of the Parameter",
            "description": "20-30 words description for the parameter",
            "weightage": 5 // INteger value on what is the importance of this parameter
            }},
            .... so on ]
        }}```
    
    Generate a JSON-based rubric system for evaluating resumes based on a given job description. 

    """


@dataclass
class AnalysisPrompt:
    SYSTEM_PROMPT = "You are an expert resume reviewer and career coach. You will evaluate the provided resume against specific evaluation criteria for a job position."

    EVALUATE_RESUME = """
    RESUME = {resume_text}

    PARAMETERS = {parameters}
    
    Please evaluate the following resume against the provided evaluation criteria. For each criterion, provide a score from 0-5 (0 being no evidence, 5 being exceptional evidence) and a 20-30 words for reason for the score.
    ## Output
    STRICTLY ADHERE TO THE FOLLOWING OUTPUT STRUCTURE:
    The output must start and trail with triple backtics.

    ```json
        {{"resume_score": [
        {{ "name": "Name of the Criteria as provided in the evaluation criteria",
        "score": 4, // Integer (0-5) based on resume match
        "justification": "Resume mentions relevant experience with [tech/skill], but lacks [specific detail]."
        }},
        ... so on
        ], }}```
    
    """
