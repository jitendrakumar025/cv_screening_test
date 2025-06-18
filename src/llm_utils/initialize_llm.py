from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
load_dotenv()

def initialize_llm(llm_type:int):
        if llm_type == 1:    
            return ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0.5)
        
   
        