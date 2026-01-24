from openai import OpenAI
from ..cache import Cache

class LLM:
    def __init__(self,
                api_key,
                base_url,
                default_model_args = None,
                use_cache = True,
                cache_dir='cache/OpenAIAPI/',
                cache_name='llm_cache'
                ):
        
        self.api_key = api_key
        self.base_url = base_url

        self.client = OpenAI(
            api_key = self.api_key, 
            base_url = self.base_url,
        )
        self.default_model_args = default_model_args if default_model_args is not None else {}

        self.use_cache = use_cache
        if self.use_cache:
            self.cache = Cache(cache_dir,cache_name)

    def generate(self,messages , model_args = None,**kwargs):
        if model_args is None:
            model_args = self.default_model_args

        if self.use_cache:
            input={'messages':messages,'model_args':model_args,'kwargs':kwargs}
            inp_encoded = self.cache.input_encode(input)
            response = self.cache.find_cache(inp_encoded)
            if response is not None:
                return response

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

        if self.use_cache:
            self.cache.save_cache(inp_encoded,response)

        return response

class Embedding:
    def __init__(self,
                api_key,
                base_url,
                default_model_args = None,
                use_cache = True,
                cache_dir='cache/OpenAIAPI/',
                cache_name='emb_cache'
                ):
        
        self.api_key = api_key
        self.base_url = base_url

        self.client = OpenAI(
            api_key = self.api_key, 
            base_url = self.base_url,
        )
        self.default_model_args = default_model_args if default_model_args is not None else {}

        self.use_cache = use_cache
        if self.use_cache:
            self.cache = Cache(cache_dir,cache_name)
    
    def generate(self,text,model_args = None,**kwargs):
        if model_args is None:
            model_args = self.default_model_args

        if self.use_cache:
            input={'text':text,'model_args':model_args,'kwargs':kwargs}
            inp_encoded = self.cache.input_encode(input)
            response = self.cache.find_cache(inp_encoded)
            if response is not None:
                return response

        response={}
        try:
            embedding = self.client.embeddings.create(
                **model_args,
                input=text,
                )
            response['status']=1
            response['embedding']=embedding.data[0].embedding
        except Exception as e:
            response['status']=0
            response['embedding']=str(e)
        
        if self.use_cache:
            self.cache.save_cache(inp_encoded,response)
        
        return response