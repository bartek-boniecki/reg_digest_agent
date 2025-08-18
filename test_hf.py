from dotenv import load_dotenv; load_dotenv()
import os, requests, json

token = os.getenv("HF_TOKEN")
model = os.getenv("HF_MODEL")

print("Token prefix:", (token or "")[:6], "…")
print("Model:", model)

# 1) Chat Completions test (works only if model is wired for chat)
url_chat = "https://api-inference.huggingface.co/v1/chat/completions"
resp = requests.post(
    url_chat,
    headers={"Authorization": f"Bearer {token}"},
    json={"model": model, "messages": [{"role": "user", "content": "Say OK once."}], "max_tokens": 20},
)
print("\nChat status:", resp.status_code)
print(resp.text[:500])

# 2) Text-generation test (universal task, but requires the model to be served)
url_gen = f"https://api-inference.huggingface.co/models/{model}"
payload = {"inputs": "Respond with: OK", "parameters": {"max_new_tokens": 20, "temperature": 0.2}}
resp2 = requests.post(url_gen, headers={"Authorization": f"Bearer {token}"}, json=payload)
print("\nGen status:", resp2.status_code)
print(resp2.text[:500])
