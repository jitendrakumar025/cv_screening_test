from src.llm_utils.initialize_llm import initialize_llm
from typing import Dict, Any
import json

################## ----------################
def extract_profile_data_sync(resume_text:str):
    try:

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
        
        llm = initialize_llm(1)

        results = llm.invoke(PROMPT)
        json_result: Dict[str, Any] = json.loads(
            results.content.strip().strip("```json").strip("```")
        )
        return json_result
    except Exception as e:
        raise ValueError(f"Error when structing resume, Reason: {e}")