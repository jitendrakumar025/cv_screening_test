from src.llm_utils.initialize_llm import initialize_llm
from typing import Dict, Any
import json


################## ----------################
def evaluate_talentPool_sync(
    detailedProfileAnalysis: str, parameters: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        # print("debug in evaluate_resume 1234")
        llm = initialize_llm(1)
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

        {detailedProfileAnalysis}

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

        results = llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )
        # print("Result in evaluaterebadnn>> ",results.content[1:10])
        return json_result
    except Exception as E:
        raise ValueError(f"Error when evaluating resume, Reason: {E}")
