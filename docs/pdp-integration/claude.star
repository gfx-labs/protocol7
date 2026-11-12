# Claude subscription login: a public client whose token endpoint takes a JSON
# body and wants the scope repeated.
def acquire(request):
    return oauth_refresh("token", parameter("client_id"), encoding="json", send_scope=True)
