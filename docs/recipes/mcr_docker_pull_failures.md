**Issue**: Docker pull for sql server image when done from WSL fails

**Reason**: Windows host prefers the resolved ipv6 address WSL prefers ipv4 address. 

**Solution**: Add an IPv4 DNS mapping to ```C:\Windows\System32\drivers\etc\hosts file```

Steps:
- Do ```nslookup``` on the below names
- Take the IP and add to the file like this 

```
x.x.x.x mcr.microsoft.com
y.y.y.y centralus.data.mcr.microsoft.com
```

Example:
``` 
150.171.70.10 mcr.microsoft.com
150.171.70.10 centralus.data.mcr.microsoft.com
```