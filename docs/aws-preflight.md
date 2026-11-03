# Free AWS Preflight

Prepared on 2026-11-03. Preparation does not authorize deployment.
See the [delivery plan](delivery-plan.md) for the deadline, scope and the
EUR 20 total gross constraint.

## Boundary

Use existing authentication and only free control-plane reads. Do not create or
change credentials, IAM policies, subscriptions, billing settings or resources.
Never ask for or print access keys, session tokens, credential files or debug logs.
Prefer an existing temporary-credential/SSO session for subsequent operation.

The free preflight deliberately excludes Cost Explorer API requests, S3 object
listing/reads, CloudWatch metric/log retrieval, Secrets Manager reads, resource
scanning activation, paid monitoring, Terraform apply and service activation.
AWS charges for Cost Explorer API requests; its ordinary console UI is free.

## Access Check

AWS CLI v2 and Terraform are installed. An existing default profile is configured.
A live `sts get-caller-identity` succeeded using a non-root identity on 2026-11-03.
Only authentication status and a masked account suffix were displayed. This does
not prove permission to inventory, inspect billing, or deploy infrastructure.
No credentials were changed or copied. Confirm this is the intended project
account privately; a valid login alone cannot establish that intention.

A reproducible terminal access check can suppress identity values entirely:

```bash
aws sts get-caller-identity --profile default --region eu-central-1 \
  --query 'contains(Arn, `:root`)' --output json --no-cli-pager
```

Exit 0 and `false` mean valid non-root authentication, not deployment approval.
For an expired session, renew login privately using the existing configured method;
never paste credentials into chat or create a long-lived key as a troubleshooting
shortcut. No access-policy changes are part of this check.

## Existing Resources

First list enabled regions with EC2 DescribeRegions. Query each enabled region,
not only the proposed Frankfurt deployment region. Record successful empty checks
separately from AccessDenied, timeout, or unsupported checks; unknown is not empty.

Initial free inventory covers:

- EC2 instance states (including stopped), EBS volumes, owned snapshots and EIPs.
- NAT gateways, classic/v2 load balancers, RDS instances and clusters.
- Global S3 bucket count and CloudFront distribution count, without object access.

### Recorded Result On 2026-11-03

| Check | Result |
|---|---|
| Authentication | Successful; non-root identity; intended account still to confirm privately |
| Regional inventory | All nine selected checks succeeded in all 17 enabled regions |
| EC2/EBS | One stopped instance and one attached volume in us-east-1 |
| Other selected regional resources | None returned; this excludes services not queried |
| S3 | One bucket; contents, storage classes and ongoing cost not inspected |
| CloudFront | Query succeeded but returned no quantity; count remains unverified |
| Credits, account plan, bills, tax and historical project spend | Unverified; available browser reached AWS sign-in |
| Public EC2 quote | Frankfurt Linux shared-tenancy t3.large: USD 0.096/hour, On-Demand |

The stopped instance's EBS storage and the S3 bucket may still incur charges.
Ownership and project attribution are unknown; no existing resources were changed.
The preflight is **partial**, not a cloud-spend approval or zero-cost certificate.

The live result is stored locally under ignored
`data/aws/preflight/2026-11-03/inventory.json`, with counts/states and public prices,
not resource identifiers, ARNs or credential values. This inventory is a selected
cost-driver check, not an exhaustive account audit: e.g. MSK/EMR/EKS/ECS, backups,
RDS snapshots, Route 53, logging, Marketplace and Support require console follow-up
if present in billing. Nothing found is stopped or deleted automatically.

Existing personal resources are not implicitly owned by this project. Before
reuse, confirm ownership, incremental cost and permission. Record a pre-project
baseline so later project spend can be separated from unrelated bills. Historical
project charges, if any, must be clarified before declaring the full EUR 20 left.

### Repeating The Regional Read Checks

List enabled regions, then repeat each selected check in every returned region:

```bash
aws ec2 describe-regions --profile default --region eu-central-1 \
  --query 'Regions[].RegionName' --output json --no-cli-pager
```

For example, inspect instance states without displaying identifiers:

```bash
aws ec2 describe-instances --profile default --region us-east-1 \
  --query 'Reservations[].Instances[].State.Name' --output json --no-cli-pager
```

The other selected reads are `ec2 describe-volumes`, `ec2 describe-snapshots
--owner-ids self`, `ec2 describe-addresses`, `ec2 describe-nat-gateways`,
`elbv2 describe-load-balancers`, `elb describe-load-balancers`,
`rds describe-db-instances`, and `rds describe-db-clusters`. Summarize only
counts/states in shared evidence, preserve automatic pagination, and never replace
failed or missing responses with zero. Global metadata checks use `s3api
list-buckets` and `cloudfront list-distributions`; no S3 objects are queried.

## Credits, Bills And Tax: Console Evidence Still Required

Use the existing AWS Console login privately. Review Billing and Cost Management:

1. Home/Free Tier: account creation/plan, eligibility and plan expiry. Do not switch
   plans or activate a feature to inspect it. Eligibility varies by account age.
2. Credits: remaining amount/currency, expiry and eligible/excluded services. Verify
   whether the balance reflects current unbilled usage; a displayed credit balance
   may update after billing. A credit is not evidence of zero future charges.
3. Bills: current November month-to-date and previous invoice, grouped by service
   and region, taxes, Support and Marketplace subscriptions. Include any continuing
   storage or commitments despite zero running instances. Repeat for December.
4. Confirm billing currency and tax treatment from the account/invoice. Record
   applicable tax and foreign-exchange assumptions without sharing billing details.

Share only a sanitized summary if needed: plan, eligible credit and expiry, current
project-attributable gross charges, continuing costs and applicable tax rate.
If billing access is denied, mark these fields unknown and resolve through the
account owner; do not treat denied access as zero spend or credit eligibility.

Use **zero credit and zero free allowance** in the conservative project estimate
until eligibility and timing are verified. Do not perform credit-earning activities
that provision services. Budget alerts alone cannot enforce the EUR 20 cap.

## Proposed Cost Envelope, Not Authorization

Working candidate: eu-central-1, Linux On-Demand x86_64, one t3.large (2 vCPU,
8 GiB), Standard CPU-credit mode, one 40 GB gp3 volume, one ephemeral public IPv4,
basic monitoring, no purchased AMI. Size must pass the local memory/load check.
Standard mode avoids surplus CPU-credit charges but may throttle sustained Spark
work. Run Spark queries sequentially; any larger machine needs a revised quote.

Plan at most **24 aggregate running hours** for all cloud sessions through Dec 11
and at most **7 aggregate days of 40 GB storage** (including stopped time).
These are total allowances across every deployment/retry, not per-session limits.
Suggested split: 8 hours integration, 8 hours retry/recreate, 8 hours final demo.
Prefer short sessions with evidence exported locally and approved teardown.

The following rates are deliberately conservative **planning assumptions, not
verified quotes**. The public EC2 quote from the read-only Pricing API is saved in
the local inventory if permissions allow. Refresh every rate before approval.

| Item | Assumed upper planning rate | Total allowance | USD |
|---|---:|---:|---:|
| EC2 Linux t3.large | 0.150/hour | 24 hours | 3.600 |
| gp3 capacity, baseline IOPS/throughput | 0.120/GB-month | 40 GB x 7/30 month | 1.120 |
| Public IPv4 | 0.005/address-hour | 24 hours | 0.120 |
| Internet egress | 0.150/GB | 1 GB | 0.150 |
| Subtotal | | | 4.990 |

Illustrative conservative conversion: 4.990 USD x 1.10 EUR/USD x 1.25 tax factor
= **EUR 6.87 gross**. Both FX and tax are assumed buffers, not statements of the
account's applicable rates. Add EUR 3.13 execution margin: **EUR 10 working
allocation**, leaving **EUR 10 unallocated contingency** within EUR 20 total.
Verified prior project costs reduce the remaining allowance. Credits can reduce
cash expenditure but do not increase the EUR 20 cap. No claim of a guaranteed
EUR 6.87 final bill is made.

Before any chargeable creation, replace assumptions with dated regional prices,
account tax/currency, exact instance/volume and total session/lifetime quantities,
all extras, estimated gross total, prior project spend and remaining allowance.
Obtain explicit approval for that concrete configuration and maximum operation
window. Recalculate if rates, size, storage duration, quantities or scope change.

Exclude NAT Gateway, paid VPC endpoints, load balancer, managed Kafka/Spark, paid
AMI/Marketplace, hosted DNS/domain, persistent EIP, remote Terraform backend,
paid observability and separate snapshots/backups from this candidate. Any added
service requires repricing and approval; its absence here is not a free allowance.

## Runtime And Teardown Review Before Deployment

- Version and locally validate Terraform/bootstrap; review the full plan and keep
  state local and ignored. Do not place API credentials in state or user-data.
- Pin the project region and resource tags. Restrict SSH to the operator IP and
  expose the result view through an authenticated SSH tunnel; keep Kafka private.
- Persist broker log data, producer state, Iceberg and independent checkpoints.
  Check available disk and memory; do not delete local data for the cloud test.
- Preserve data on stop/reboot. Explicitly decide deletion behavior on termination.
- Bound ingestion cycles and Spark runs. Add a local shutdown timer as a secondary
  guard, with the limitation that host failure or timer failure can prevent action.
- Stopping EC2 leaves storage charges. Stopping is not full teardown. Account for
  every hour/day retained, export evidence, then follow separately authorized
  termination/volume deletion. Never remove unrelated existing resources.
- After teardown, re-inventory project instances, EBS, snapshots and addresses;
  check bills again after AWS usage posting delays and after the final session.
- Maintain an aggregate run/storage/expense ledger under ignored data/. Stop new
  sessions if actual usage, remaining budget or billing state is unclear.

## Preflight Exit Gate

Ready for implementation planning when the intended account, authentication,
resource baseline, required permission gaps, billing/credit/tax status, candidate
cost envelope and Terraform/runtime tasks are explicit. Unknown billing does not
block cost-free local work, but blocks treating cloud spend as approved.

The free preflight does not certify deploy permissions, zero account liability,
complete inventory coverage, CPU/RAM sufficiency or a hard AWS spending cap.

## Official References

- [STS identity check](https://docs.aws.amazon.com/cli/latest/reference/sts/get-caller-identity.html)
- [Cost Explorer API pricing](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/pricing/)
- [Free Cost Explorer console UI](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html)
- [AWS credits and billing balance](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/useconsolidatedbilling-credits.html)
- [Free Tier account plans](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html)
- [EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/)
- [T3 credit modes](https://aws.amazon.com/ec2/instance-types/t3/)
- [EBS pricing](https://aws.amazon.com/ebs/pricing/)
- [Public IPv4 pricing](https://aws.amazon.com/vpc/pricing/)
- [Price List query API](https://docs.aws.amazon.com/cli/latest/reference/pricing/get-products.html)
