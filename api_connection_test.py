import http.client
import os

conn = http.client.HTTPSConnection("kroo.mambu.com")
payload = ''
auth = os.environ.get('MAMBU_AUTH', '')

# Test 1: OLD check_access (v1 + settings/organization) - should return 410
print("=== OLD: v1 + /api/settings/organization ===")
headers_v1 = {
  'Accept': 'application/vnd.mambu.v1+json',
  'Authorization': f'Basic {auth}'
}
conn.request("GET", "/api/settings/organization", payload, headers_v1)
res = conn.getresponse()
print(f"Status: {res.status} {res.reason}")
data = res.read()
print(data.decode("utf-8")[:500])

conn = http.client.HTTPSConnection("kroo.mambu.com")

# Test 2: NEW check_access (v2 + branches) - should return 200
print("\n=== NEW: v2 + /api/branches ===")
headers_v2 = {
  'Accept': 'application/vnd.mambu.v2+json',
  'Authorization': f'Basic {auth}'
}
conn.request("GET", "/api/branches", payload, headers_v2)
res = conn.getresponse()
print(f"Status: {res.status} {res.reason}")
data = res.read()
print(data.decode("utf-8")[:500])