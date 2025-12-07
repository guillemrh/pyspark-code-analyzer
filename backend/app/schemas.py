from pydantic import BaseModel

class CodeInput(BaseModel):
    code: str

class ExplanationOutput(BaseModel):
    explanation: str