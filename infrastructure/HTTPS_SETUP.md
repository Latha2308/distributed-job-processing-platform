# HTTPS Configuration Requirements

## Current Status
⚠️ **HTTP-only configuration is NOT production-secure**
API traffic is unencrypted and vulnerable to interception.

## Required Before Public API Exposure

### 1. Domain Name
**Required**: Register a domain name
- **Options**: Route53, GoDaddy, Namecheap, etc.
- **Recommendation**: Use Route53 for seamless AWS integration
- **Cost**: ~$12-15/year for .com domain

### 2. AWS Certificate Manager (ACM) Certificate
**Required**: SSL/TLS certificate for your domain
- **Process**:
  ```bash
  # Request certificate via AWS Console or CLI
  aws acm request-certificate \
    --domain-name api.yourdomain.com \
    --validation-method DNS \
    --region us-east-1
  ```
- **Validation**: Add CNAME record to DNS (automated if using Route53)
- **Cost**: FREE with ACM
- **Important**: Certificate must be in same region as ALB (us-east-1)

### 3. DNS Configuration
**Required**: Point domain to ALB
- **Record Type**: CNAME or A (alias)
- **Example**:
  ```
  api.yourdomain.com -> job-processing-prod-alb-1234567890.us-east-1.elb.amazonaws.com
  ```
- **Route53 Alias** (recommended):
  - Type: A record
  - Alias: Yes
  - Target: ALB DNS name
  - Cost: $0.50/month for hosted zone + $0.40/million queries

### 4. Terraform Configuration
**Required**: Enable HTTPS in terraform.tfvars
```hcl
enable_https        = true
domain_name         = "api.yourdomain.com"
acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/abcd-1234-..."
```

## Step-by-Step Setup Process

### Option A: Using Route53 (Recommended)
1. **Register domain in Route53** (~5 minutes)
   - AWS Console → Route53 → Register Domain
   - Cost: ~$12/year

2. **Request ACM certificate** (~2 minutes)
   - AWS Console → Certificate Manager → Request certificate
   - Domain: api.yourdomain.com
   - Validation: DNS
   - If using Route53: Click "Create records in Route53" (automated)

3. **Wait for validation** (~5-30 minutes)
   - Certificate status will change from "Pending validation" to "Issued"

4. **Deploy infrastructure** (after terraform apply)
   - Get ALB DNS name from Terraform outputs

5. **Create Route53 alias record** (~1 minute)
   ```bash
   # Get hosted zone ID
   ZONE_ID=$(aws route53 list-hosted-zones-by-name --dns-name yourdomain.com --query 'HostedZones[0].Id' --output text)

   # Get ALB details from Terraform
   ALB_DNS=$(cd infrastructure/terraform && terraform output -raw alb_dns_name)
   ALB_ZONE=$(aws elbv2 describe-load-balancers --query 'LoadBalancers[0].CanonicalHostedZoneId' --output text)

   # Create alias record
   aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch '{
     "Changes": [{
       "Action": "CREATE",
       "ResourceRecordSet": {
         "Name": "api.yourdomain.com",
         "Type": "A",
         "AliasTarget": {
           "HostedZoneId": "'$ALB_ZONE'",
           "DNSName": "'$ALB_DNS'",
           "EvaluateTargetHealth": true
         }
       }
     }]
   }'
   ```

6. **Update Terraform configuration**
   - Edit `terraform.tfvars`:
     ```hcl
     enable_https = true
     domain_name = "api.yourdomain.com"
     acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/..."
     ```
   - Run `terraform apply` to update ALB listener

7. **Test HTTPS**
   ```bash
   curl https://api.yourdomain.com/health
   ```

### Option B: Using External Domain Registrar
1. **Register domain** with your preferred registrar

2. **Create Route53 hosted zone** (optional but recommended)
   ```bash
   aws route53 create-hosted-zone --name yourdomain.com --caller-reference $(date +%s)
   ```

3. **Update domain nameservers** at registrar
   - Point to Route53 nameservers (from hosted zone)
   - Wait 24-48 hours for propagation

4. **Follow steps 2-7 from Option A**

## Security Considerations

### Before Public Exposure
- [ ] HTTPS enabled with valid certificate
- [ ] Domain validated and DNS configured
- [ ] WAF configured for DDoS protection (optional but recommended)
- [ ] Rate limiting configured
- [ ] API authentication implemented
- [ ] Input validation enabled
- [ ] Error messages don't leak sensitive info

### Additional Recommendations
1. **Enable WAF** (~$5-20/month)
   - Protects against common web exploits
   - Rate limiting
   - IP reputation lists

2. **Enable CloudFront** (optional, for caching/edge delivery)
   - Reduces latency
   - Additional DDoS protection
   - Cost: Pay-per-use

3. **Enable GuardDuty** (~$30-50/month for small deployment)
   - Threat detection
   - Monitors API calls for suspicious activity

4. **CloudWatch Alarms**
   - 4xx/5xx error rates
   - Request count spikes
   - Unhealthy targets

## Cost Impact of HTTPS

| Component | Cost | Notes |
|-----------|------|-------|
| Domain (Route53) | ~$12/year | One-time annual cost |
| ACM Certificate | FREE | Managed by AWS |
| Route53 Hosted Zone | $0.50/month | If using Route53 |
| Route53 Queries | ~$0.40/million | Negligible for most apps |
| ALB HTTPS | $0.00 extra | No additional cost vs HTTP |
| **Total Additional** | **~$0.50/month** | Plus domain registration |

## Testing Without Domain

For development/testing, you can:
1. Deploy with HTTP only (current configuration)
2. Test using ALB DNS name: `http://job-processing-prod-alb-xxx.us-east-1.elb.amazonaws.com`
3. Add HTTPS later when ready for production

**Important**: Never expose production APIs over HTTP - all traffic is plaintext and vulnerable.

## Minimum Production Requirements

✅ **Required for production**:
- Domain name with HTTPS/ACM certificate
- WAF or rate limiting
- Authentication/authorization
- Input validation
- Monitoring and alerting

⚠️ **Not sufficient for production**:
- HTTP-only deployment
- No authentication
- No rate limiting
- No monitoring
