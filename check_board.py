import urllib.request, urllib.error, json
TOKEN = "eyJtaXJvLm9yaWdpbiI6ImV1MDEifQ_jbnCM-R9eAU0j9EFV4pY7IwO3QI"

for bid in ["uXjVG-EZxEk=", "uXjVG-I0l5g="]:
    req = urllib.request.Request(
        f"https://api.miro.com/v2/boards/{bid}",
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read())
        print(f"{bid}: {data['name']} | team: {data.get('team',{}).get('id','?')} | created: {data.get('createdAt','?')}")
    except urllib.error.HTTPError as e:
        print(f"{bid}: HTTP {e.code} - {e.read().decode()[:200]}")
