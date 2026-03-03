import urllib.request, json
TOKEN = "eyJtaXJvLm9yaWdpbiI6ImV1MDEifQ_jbnCM-R9eAU0j9EFV4pY7IwO3QI"
req = urllib.request.Request("https://api.miro.com/v2/boards", headers={"Authorization": f"Bearer {TOKEN}"})
resp = urllib.request.urlopen(req)
data = json.loads(resp.read())
for b in data["data"]:
    print(f"  {b['id']:20}  {b['name']}")
print(f"\nTotal: {len(data['data'])} boards")
