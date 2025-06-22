# AWS SES DNS Records for squarepeg.vc

Add these DNS records to your domain's DNS settings (Google Domains, Cloudflare, etc.)

## 1. Domain Verification (TXT Record)

**Type:** TXT  
**Name:** `_amazonses.squarepeg.vc`  
**Value:** `91cymi8X0vzKV5uZdbQpZ8b8AVadfXPDoqw1+dXJZvs=`  
**TTL:** 300 (or default)

## 2. DKIM Records (CNAME Records)

### DKIM Record 1:
**Type:** CNAME  
**Name:** `djtl45epywrgzjhtscc2agnyuebsaplo._domainkey.squarepeg.vc`  
**Value:** `djtl45epywrgzjhtscc2agnyuebsaplo.dkim.amazonses.com`  
**TTL:** 300 (or default)

### DKIM Record 2:
**Type:** CNAME  
**Name:** `wnpvoq2kvh2wey2zigivp2kuxix33ddq._domainkey.squarepeg.vc`  
**Value:** `wnpvoq2kvh2wey2zigivp2kuxix33ddq.dkim.amazonses.com`  
**TTL:** 300 (or default)

### DKIM Record 3:
**Type:** CNAME  
**Name:** `rry2hq6ydjumypq3f7aok7ry7zfkbsoh._domainkey.squarepeg.vc`  
**Value:** `rry2hq6ydjumypq3f7aok7ry7zfkbsoh.dkim.amazonses.com`  
**TTL:** 300 (or default)

## 3. SPF Record (Optional - Check Existing First!)

⚠️ **WARNING**: You likely already have an SPF record for Google Workspace. 
Check your existing TXT records first!

If you have an existing SPF record like:
```
v=spf1 include:_spf.google.com ~all
```

Update it to include AWS SES:
```
v=spf1 include:_spf.google.com include:amazonses.com ~all
```

**DO NOT** create a second SPF record - domains can only have one SPF record.

## 4. Custom MAIL FROM Domain (Optional but Recommended)

This removes "via amazonses.com" from Gmail's interface.

**Type:** MX  
**Name:** `bounce.squarepeg.vc`  
**Value:** `10 feedback-smtp.us-east-1.amazonses.com`  
**TTL:** 300 (or default)

**Type:** TXT  
**Name:** `bounce.squarepeg.vc`  
**Value:** `v=spf1 include:amazonses.com ~all`  
**TTL:** 300 (or default)

---

## Verification Commands

After adding the DNS records, wait 5-10 minutes, then run:

```bash
# Check domain verification status
aws ses get-identity-verification-attributes \
  --identities squarepeg.vc \
  --profile haystack-prod \
  --region us-east-1

# Check DKIM verification status  
aws ses get-identity-dkim-attributes \
  --identities squarepeg.vc \
  --profile haystack-prod \
  --region us-east-1
```

Both should show "VerificationStatus": "Success" when properly configured. 