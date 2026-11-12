# Codex login with a ChatGPT account: a public client, form body, no scope.
def acquire(request):
    return oauth_refresh("token", parameter("client_id"))
