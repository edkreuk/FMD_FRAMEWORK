import urllib.request
TOKEN = "eyJtaXJvLm9yaWdpbiI6ImV1MDEifQ_jbnCM-R9eAU0j9EFV4pY7IwO3QI"
req = urllib.request.Request(
    "https://api.miro.com/v2/boards/uXjVG-I0l5g=",
    headers={"Authorization": f"Bearer {TOKEN}"},
    method="DELETE"
)
try:
    resp = urllib.request.urlopen(req)
    print(f"Deleted: {resp.status}")
except Exception as e:
    print(f"Error: {e}")
