# AWS Single-Host Deployment

The Terraform configuration in `infra/aws/` defines one On-Demand EC2 host in
Frankfurt (`eu-central-1`). It matches the candidate in the
[AWS preflight](aws-preflight.md#proposed-cost-envelope-not-authorization).
Creation requires a reviewed live plan, an updated cost calculation and explicit
approval of the total runtime and storage allowance.

## Local Validation

Use Terraform 1.14.x (tested with 1.14.9). The lock file pins the signed AWS
provider and includes checksums for macOS ARM64 and Linux AMD64.

```bash
terraform -chdir=infra/aws init -backend=false -input=false -lockfile=readonly
terraform -chdir=infra/aws fmt -check -recursive
terraform -chdir=infra/aws validate
terraform -chdir=infra/aws test
bash -n infra/aws/bootstrap.sh
```

The tests use a mock AWS provider and require no AWS credentials. They check the
host size, credit mode, storage, metadata options and restricted SSH ingress.
A mock plan cannot verify the target account's permissions, available capacity,
AMI or first boot.

## Review Inputs And The Live Plan

Keep the approved input values in an ignored `data/terraform/demo.tfvars` file.
Select the same AWS CLI profile used in the preflight. Terraform checks that its
active credentials belong to the specified AWS account.

| Input | Required value |
|---|---|
| `aws_account_id` | Approved 12-digit AWS account ID |
| `ami_id` | Exact reviewed public Canonical Ubuntu 24.04 LTS AMD64 server AMI ID in Frankfurt |
| `operator_ipv4_cidr` | Current public IPv4 address of the operator, with `/32` |
| `ssh_public_key` | One OpenSSH Ed25519 or RSA public key line |

Use Canonical's [AMI lookup instructions](https://documentation.ubuntu.com/aws/aws-how-to/instances/launch-ubuntu-ec2-instance/)
to select a versioned AMI. Terraform filters the supplied ID by owner, public
visibility, Ubuntu edition, architecture and availability. Leave the OpenAQ key,
SSH private key and AWS credentials outside Terraform. Local state, input files
and saved plans are ignored and should be retained privately until teardown is
verified.

```bash
terraform -chdir=infra/aws plan -input=false \
  -var-file="$(pwd)/data/terraform/demo.tfvars" \
  -out="$(pwd)/data/terraform/demo.tfplan"
terraform -chdir=infra/aws show "$(pwd)/data/terraform/demo.tfplan"
```

Review the plan and account before requesting provisioning approval. The expected
shape is one `t3.large` with Standard CPU credits, an encrypted 40 GiB gp3 root
volume at baseline performance, basic monitoring, and one ephemeral IPv4. A
dedicated VPC, subnet and internet gateway provide routing. The security group
admits TCP 22 from the approved operator address and permits outbound web
traffic. Kafka and the result view bind to loopback and the operator reaches
the view through SSH port forwarding.

## First Boot And Lifecycle

Cloud-init installs Ubuntu's Docker Engine and Compose plugin, Python 3.12
virtual-environment support and Java 21. Ubuntu supplies package security
updates within the pinned OS release. Record the installed package versions
during the Linux integration check. After an approved deployment, check
`cloud-init status --wait --long`, `docker compose version`, `python3.12 --version`
and `java -version` before transferring the reviewed application revision. Use a
new SSH login after cloud-init finishes for Docker group membership.
Keep the checkout path fixed and retain the Kafka directory, producer state,
checkpoints and Iceberg warehouse under its `data/` directory.

The bootstrap arms a systemd timer before installing packages. It requests
poweroff eight hours after boot, which maps to an EC2 stop. Verify the timer
with `systemctl list-timers environmental-demo-stop.timer` and track elapsed time
from the operator side. A reboot resets the timer. The timer can fail with the
host or bootstrap, and stopped EBS storage continues to incur charges. Follow the
aggregate runtime and storage allowance in the preflight across all sessions.

When the result view is running on the host, open an SSH tunnel using the
current EC2 address in `demo_ipv4`.

```bash
ssh -N -L 127.0.0.1:8765:127.0.0.1:8765 "ubuntu@$demo_ipv4"
```

The root volume survives stop/start and reboot. After stop/start, read the new
ephemeral IPv4 from EC2 or refresh Terraform state before reconnecting. Start the
broker and run the bounded jobs explicitly. Before separately authorized
termination, export evidence and any data required for reproduction. Terminating
the instance deletes its root volume. Confirm the project resources are gone and
review delayed billing. See [EC2 lifecycle behavior](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html).

The live plan, Linux runtime sizing, service restart and host reboot checks remain
deployment acceptance steps.
