from openai import OpenAI
from ..cache import Cache

class LLM:
    def __init__(self,api_key,base_url,default_model_args:dict={}):
        self.api_key = api_key
        self.base_url = base_url

        self.client = OpenAI(
            api_key = self.api_key, 
            base_url = self.base_url,
        )
        self.default_model_args = default_model_args

    @Cache(cache_dir='cache/OpenAIAPI/',cache_name='llm_cache')
    def generate(self,messages,model_args,**kwargs):

        response={}        
        try:
            completion = self.client.chat.completions.create(
                **model_args,
                messages=messages,
            )
            response['status']=1
            response['answer']=completion.choices[0].message.content
        except Exception as e:
            response['status']=0
            response['answer']= str(e)

        try:
            response['usage_prompt_tokens']=completion.usage.prompt_tokens
            response['usage_completion_tokens']=completion.usage.completion_tokens
            response['usage_total_tokens']=completion.usage.total_tokens
        except:
            response['usage_prompt_tokens']=None
            response['usage_completion_tokens']=None
            response['usage_total_tokens']=None

        try:
            response['usage_cached_tokens']=completion.usage.prompt_tokens_details.cached_tokens
        except:
            response['usage_cached_tokens']=None

        return response

class Embedding:
    def __init__(self,api_key,base_url,default_model_args:dict={}):
        self.api_key = api_key
        self.base_url = base_url

        self.client = OpenAI(
            api_key = self.api_key, 
            base_url = self.base_url,
        )
        
        self.default_model_args = default_model_args
    
    @Cache(cache_dir='cache/OpenAIAPI/',cache_name='emb_cache')
    def generate(self,prompt,model_args,**kwargs):
        response={}
        try:
            embedding = self.client.embeddings.create(
                **model_args,
                input=prompt,
                )
            response['status']=1
            response['answer']=embedding.data[0].embedding
        except Exception as e:
            response['status']=0
            response['answer']=str(e)

        return response