package cfn2tf

import (
	"fmt"
	"sort"
	"strings"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cfn"
)

// props reads the properties of a resource (or of a nested object) and remembers which ones were
// read, so anything the converter does not understand is reported instead of silently dropped.
type props struct {
	c     *conv
	r     cfn.Resource
	where string
	m     map[string]any
	used  map[string]bool
}

func (p *props) child(k string, m map[string]any) *props {
	return &props{c: p.c, r: p.r, where: p.where + "." + k, m: m, used: map[string]bool{}}
}

// resolve picks the branch of Fn::If around a whole value; AWS::NoValue counts as absent.
func (p *props) resolve(v any) (any, bool) {
	for {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			return v, v != nil
		}
		if r, ok := m["Ref"].(string); ok && r == "AWS::NoValue" {
			return nil, false
		}
		arg, ok := m["Fn::If"].([]any)
		if !ok || len(arg) != 3 {
			return v, true
		}
		name, _ := arg[0].(string)
		if p.c.conds[name] {
			v = arg[1]
		} else {
			v = arg[2]
		}
	}
}

func (p *props) raw(k string) (any, bool) {
	p.used[k] = true
	v, ok := p.m[k]
	if !ok {
		return nil, false
	}
	return p.resolve(v)
}

func (p *props) has(k string) bool { _, ok := p.raw(k); return ok }

// get converts a property to an expression (nil when absent).
func (p *props) get(k string) Expr {
	v, ok := p.raw(k)
	if !ok {
		return nil
	}
	e := p.c.value(v, p.where+"."+k)
	if _, skip := e.(noValue); skip {
		return nil
	}
	return e
}

// json converts a property that Terraform takes as a JSON string (policies, redrive policies).
func (p *props) json(k string) Expr {
	e := p.get(k)
	if e == nil {
		return nil
	}
	if s, ok := e.(string); ok { // already a JSON string in the template
		return s
	}
	return Call{"jsonencode", []Expr{e}}
}

func (p *props) sub(k string) *props {
	v, ok := p.raw(k)
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		p.c.fail("%s.%s must be an object", p.where, k)
		return nil
	}
	return p.child(k, m)
}

func (p *props) list(k string) []*props {
	v, ok := p.raw(k)
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		p.c.fail("%s.%s must be a list", p.where, k)
		return nil
	}
	var out []*props
	for i, e := range arr {
		e, ok := p.resolve(e)
		if !ok {
			continue
		}
		m, ok := e.(map[string]any)
		if !ok {
			p.c.fail("%s.%s[%d] must be an object", p.where, k, i)
			continue
		}
		out = append(out, p.child(fmt.Sprintf("%s[%d]", k, i), m))
	}
	return out
}

// strings returns a list property as expressions.
func (p *props) items(k string) []Expr {
	e := p.get(k)
	if e == nil {
		return nil
	}
	arr, ok := e.([]Expr)
	if !ok {
		p.c.fail("%s.%s must be a list", p.where, k)
		return nil
	}
	return arr
}

func (p *props) checkUnused() {
	var extra []string
	for k := range p.m {
		if !p.used[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	for _, k := range extra {
		p.c.fail("%s.%s: property is not supported for %s", p.where, k, p.r.Type)
	}
}

// done checks a nested object.
func (p *props) done() {
	if p != nil {
		p.checkUnused()
	}
}

// tags converts [{Key, Value}] to a map.
func (p *props) tags(k string) Expr {
	list := p.list(k)
	if list == nil {
		return nil
	}
	o := NewObject()
	for _, t := range list {
		key, _ := t.raw("Key")
		ks, ok := key.(string)
		if !ok {
			p.c.fail("%s: tag keys must be literal strings", t.where)
			continue
		}
		o.Set(ks, t.get("Value"))
		t.done()
	}
	return o
}

func (p *props) physical() string { return p.c.in.Physical[p.r.LogicalID] }

func importBlock(addr, id string) *Block {
	return (&Block{Type: "import"}).Set("to", Raw(addr)).Set("id", id)
}

func resource(typ, name string) *Block { return &Block{Type: "resource", Labels: []string{typ, name}} }

// nameOr is the name property if set, else the physical name, so Terraform never renames
// (which would replace) what CloudFormation generated.
func (p *props) nameOr(k, physical string) Expr {
	if e := p.get(k); e != nil {
		return e
	}
	return physical
}

type mapper struct {
	needsPhysical bool
	ref           func(name string) ref
	convert       func(c *conv, p *props, name string) (blocks, imports []*Block)
}

var mappers = map[string]mapper{
	"AWS::S3::Bucket":         {true, s3Ref, s3Bucket},
	"AWS::S3::BucketPolicy":   {false, noRef("aws_s3_bucket_policy"), s3BucketPolicy},
	"AWS::SQS::Queue":         {true, sqsRef, sqsQueue},
	"AWS::SQS::QueuePolicy":   {false, noRef("aws_sqs_queue_policy"), sqsQueuePolicy},
	"AWS::SNS::Topic":         {true, snsRef, snsTopic},
	"AWS::SNS::Subscription":  {true, simpleRef("aws_sns_topic_subscription", "arn", nil), snsSubscription},
	"AWS::IAM::Role":          {true, roleRef, iamRole},
	"AWS::IAM::Policy":        {false, noRef("aws_iam_role_policy"), iamPolicy},
	"AWS::IAM::ManagedPolicy": {true, simpleRef("aws_iam_policy", "arn", map[string]string{"PolicyArn": "arn"}), iamManagedPolicy},
	"AWS::Logs::LogGroup":     {true, simpleRef("aws_cloudwatch_log_group", "name", map[string]string{"Arn": "arn|:*"}), logGroup},
	"AWS::SSM::Parameter":     {true, simpleRef("aws_ssm_parameter", "name", map[string]string{"Type": "type", "Value": "value"}), ssmParameter},
	"AWS::DynamoDB::Table":    {true, simpleRef("aws_dynamodb_table", "name", map[string]string{"Arn": "arn", "StreamArn": "stream_arn"}), dynamoTable},
}

func simpleRef(typ, refAttr string, atts map[string]string) func(string) ref {
	return func(name string) ref { return ref{addr: typ + "." + name, ref: refAttr, getAtt: atts} }
}

func noRef(typ string) func(string) ref {
	return func(name string) ref { return ref{addr: typ + "." + name} }
}

// ---- S3 ----

func s3Ref(name string) ref {
	return ref{addr: "aws_s3_bucket." + name, ref: "id", getAtt: map[string]string{
		"Arn": "arn", "DomainName": "bucket_domain_name", "RegionalDomainName": "bucket_regional_domain_name"}}
}

func s3Bucket(c *conv, p *props, name string) (bs, ims []*Block) {
	bucket := p.physical()
	b := resource("aws_s3_bucket", name).Set("bucket", p.nameOr("BucketName", bucket)).Set("tags", p.tags("Tags"))
	bs = append(bs, b)
	ims = append(ims, importBlock("aws_s3_bucket."+name, bucket))
	ref := Raw("aws_s3_bucket." + name + ".id")

	if v := p.sub("VersioningConfiguration"); v != nil {
		r := resource("aws_s3_bucket_versioning", name).Set("bucket", ref)
		r.Add("versioning_configuration").Set("status", v.get("Status"))
		v.done()
		bs = append(bs, r)
		ims = append(ims, importBlock("aws_s3_bucket_versioning."+name, bucket))
	}
	if e := p.sub("BucketEncryption"); e != nil {
		r := resource("aws_s3_bucket_server_side_encryption_configuration", name).Set("bucket", ref)
		for _, rule := range e.list("ServerSideEncryptionConfiguration") {
			rb := r.Add("rule")
			if d := rule.sub("ServerSideEncryptionByDefault"); d != nil {
				rb.Add("apply_server_side_encryption_by_default").Set("sse_algorithm", d.get("SSEAlgorithm")).Set("kms_master_key_id", d.get("KMSMasterKeyID"))
				d.done()
			}
			rb.Set("bucket_key_enabled", rule.get("BucketKeyEnabled"))
			rule.done()
		}
		e.done()
		bs = append(bs, r)
		ims = append(ims, importBlock("aws_s3_bucket_server_side_encryption_configuration."+name, bucket))
	}
	if l := p.sub("LifecycleConfiguration"); l != nil {
		r := resource("aws_s3_bucket_lifecycle_configuration", name).Set("bucket", ref)
		for _, rule := range l.list("Rules") {
			rb := r.Add("rule").Set("id", rule.get("Id")).Set("status", rule.get("Status"))
			if pre := rule.get("Prefix"); pre != nil {
				rb.Add("filter").Set("prefix", pre)
			} else {
				rb.Add("filter")
			}
			if d := rule.get("ExpirationInDays"); d != nil {
				rb.Add("expiration").Set("days", d)
			}
			if d := rule.get("NoncurrentVersionExpirationInDays"); d != nil {
				rb.Add("noncurrent_version_expiration").Set("noncurrent_days", d)
			}
			for _, t := range rule.list("Transitions") {
				rb.Add("transition").Set("days", t.get("TransitionInDays")).Set("storage_class", t.get("StorageClass"))
				t.done()
			}
			if a := rule.sub("AbortIncompleteMultipartUpload"); a != nil {
				rb.Add("abort_incomplete_multipart_upload").Set("days_after_initiation", a.get("DaysAfterInitiation"))
				a.done()
			}
			rule.done()
		}
		l.done()
		bs = append(bs, r)
		ims = append(ims, importBlock("aws_s3_bucket_lifecycle_configuration."+name, bucket))
	}
	if a := p.sub("PublicAccessBlockConfiguration"); a != nil {
		r := resource("aws_s3_bucket_public_access_block", name).Set("bucket", ref).
			Set("block_public_acls", a.get("BlockPublicAcls")).Set("block_public_policy", a.get("BlockPublicPolicy")).
			Set("ignore_public_acls", a.get("IgnorePublicAcls")).Set("restrict_public_buckets", a.get("RestrictPublicBuckets"))
		a.done()
		bs = append(bs, r)
		ims = append(ims, importBlock("aws_s3_bucket_public_access_block."+name, bucket))
	}
	return bs, ims
}

func s3BucketPolicy(c *conv, p *props, name string) (bs, ims []*Block) {
	bucketRaw, _ := p.raw("Bucket")
	r := resource("aws_s3_bucket_policy", name).Set("bucket", p.get("Bucket")).Set("policy", p.json("PolicyDocument"))
	return []*Block{r}, []*Block{importBlock("aws_s3_bucket_policy."+name, c.staticID(bucketRaw, p.where+".Bucket"))}
}

// ---- SQS ----

func sqsRef(name string) ref {
	return ref{addr: "aws_sqs_queue." + name, ref: "url", getAtt: map[string]string{"Arn": "arn", "QueueName": "name", "QueueUrl": "url"}}
}

func sqsQueue(c *conv, p *props, name string) (bs, ims []*Block) {
	url := p.physical()
	physicalName := url[strings.LastIndex(url, "/")+1:]
	r := resource("aws_sqs_queue", name).Set("name", p.nameOr("QueueName", physicalName)).
		Set("fifo_queue", p.get("FifoQueue")).
		Set("content_based_deduplication", p.get("ContentBasedDeduplication")).
		Set("delay_seconds", p.get("DelaySeconds")).
		Set("max_message_size", p.get("MaximumMessageSize")).
		Set("message_retention_seconds", p.get("MessageRetentionPeriod")).
		Set("receive_wait_time_seconds", p.get("ReceiveMessageWaitTimeSeconds")).
		Set("visibility_timeout_seconds", p.get("VisibilityTimeout")).
		Set("sqs_managed_sse_enabled", p.get("SqsManagedSseEnabled")).
		Set("kms_master_key_id", p.get("KmsMasterKeyId")).
		Set("kms_data_key_reuse_period_seconds", p.get("KmsDataKeyReusePeriodSeconds")).
		Set("redrive_policy", p.json("RedrivePolicy")).
		Set("redrive_allow_policy", p.json("RedriveAllowPolicy")).
		Set("tags", p.tags("Tags"))
	return []*Block{r}, []*Block{importBlock("aws_sqs_queue."+name, url)}
}

func sqsQueuePolicy(c *conv, p *props, name string) (bs, ims []*Block) {
	queuesRaw, _ := p.raw("Queues")
	list, _ := queuesRaw.([]any)
	queues := p.items("Queues")
	policy := p.json("PolicyDocument")
	for i, q := range queues {
		n := name
		if len(queues) > 1 {
			n = fmt.Sprintf("%s_%d", name, i)
		}
		bs = append(bs, resource("aws_sqs_queue_policy", n).Set("queue_url", q).Set("policy", policy))
		ims = append(ims, importBlock("aws_sqs_queue_policy."+n, c.staticID(list[i], fmt.Sprintf("%s.Queues[%d]", p.where, i))))
	}
	return bs, ims
}

// ---- SNS ----

func snsRef(name string) ref {
	return ref{addr: "aws_sns_topic." + name, ref: "arn", getAtt: map[string]string{"TopicArn": "arn", "TopicName": "name"}}
}

func snsTopic(c *conv, p *props, name string) (bs, ims []*Block) {
	arn := p.physical()
	if p.has("Subscription") {
		c.fail("%s.Subscription: inline subscriptions have no IDs of their own; declare them as AWS::SNS::Subscription first", p.where)
	}
	r := resource("aws_sns_topic", name).Set("name", p.nameOr("TopicName", arn[strings.LastIndex(arn, ":")+1:])).
		Set("display_name", p.get("DisplayName")).
		Set("fifo_topic", p.get("FifoTopic")).
		Set("content_based_deduplication", p.get("ContentBasedDeduplication")).
		Set("kms_master_key_id", p.get("KmsMasterKeyId")).
		Set("tags", p.tags("Tags"))
	return []*Block{r}, []*Block{importBlock("aws_sns_topic."+name, arn)}
}

func snsSubscription(c *conv, p *props, name string) (bs, ims []*Block) {
	r := resource("aws_sns_topic_subscription", name).
		Set("topic_arn", p.get("TopicArn")).
		Set("protocol", p.get("Protocol")).
		Set("endpoint", p.get("Endpoint")).
		Set("raw_message_delivery", p.get("RawMessageDelivery")).
		Set("filter_policy", p.json("FilterPolicy")).
		Set("filter_policy_scope", p.get("FilterPolicyScope")).
		Set("redrive_policy", p.json("RedrivePolicy"))
	return []*Block{r}, []*Block{importBlock("aws_sns_topic_subscription."+name, p.physical())}
}

// ---- IAM ----

func roleRef(name string) ref {
	return ref{addr: "aws_iam_role." + name, ref: "name", getAtt: map[string]string{"Arn": "arn", "RoleId": "unique_id"}}
}

func iamRole(c *conv, p *props, name string) (bs, ims []*Block) {
	roleName := p.physical()
	r := resource("aws_iam_role", name).Set("name", p.nameOr("RoleName", roleName)).
		Set("path", p.get("Path")).
		Set("description", p.get("Description")).
		Set("max_session_duration", p.get("MaxSessionDuration")).
		Set("permissions_boundary", p.get("PermissionsBoundary")).
		Set("assume_role_policy", p.json("AssumeRolePolicyDocument")).
		Set("tags", p.tags("Tags"))
	bs = append(bs, r)
	ims = append(ims, importBlock("aws_iam_role."+name, roleName))
	role := Raw("aws_iam_role." + name + ".name")
	for _, pol := range p.list("Policies") {
		pn, _ := pol.raw("PolicyName")
		policyName := c.staticID(pn, pol.where+".PolicyName")
		n := name + "_" + snake(policyName)
		bs = append(bs, resource("aws_iam_role_policy", n).Set("name", pol.get("PolicyName")).Set("role", role).Set("policy", pol.json("PolicyDocument")))
		ims = append(ims, importBlock("aws_iam_role_policy."+n, roleName+":"+policyName))
		pol.done()
	}
	arnsRaw, _ := p.raw("ManagedPolicyArns")
	rawList, _ := arnsRaw.([]any)
	for i, arn := range p.items("ManagedPolicyArns") {
		n := fmt.Sprintf("%s_%d", name, i)
		bs = append(bs, resource("aws_iam_role_policy_attachment", n).Set("role", role).Set("policy_arn", arn))
		ims = append(ims, importBlock("aws_iam_role_policy_attachment."+n, roleName+"/"+c.staticID(rawList[i], fmt.Sprintf("%s.ManagedPolicyArns[%d]", p.where, i))))
	}
	return bs, ims
}

func iamPolicy(c *conv, p *props, name string) (bs, ims []*Block) {
	if p.has("Users") || p.has("Groups") {
		c.fail("%s: policies for users or groups are not supported, only Roles", p.where)
	}
	pn, _ := p.raw("PolicyName")
	policyName := c.staticID(pn, p.where+".PolicyName")
	rolesRaw, _ := p.raw("Roles")
	rawList, _ := rolesRaw.([]any)
	policy := p.json("PolicyDocument")
	for i, role := range p.items("Roles") {
		n := name
		if len(rawList) > 1 {
			n = fmt.Sprintf("%s_%d", name, i)
		}
		bs = append(bs, resource("aws_iam_role_policy", n).Set("name", p.get("PolicyName")).Set("role", role).Set("policy", policy))
		ims = append(ims, importBlock("aws_iam_role_policy."+n, c.staticID(rawList[i], fmt.Sprintf("%s.Roles[%d]", p.where, i))+":"+policyName))
	}
	return bs, ims
}

func iamManagedPolicy(c *conv, p *props, name string) (bs, ims []*Block) {
	arn := p.physical()
	r := resource("aws_iam_policy", name).Set("name", p.nameOr("ManagedPolicyName", arn[strings.LastIndex(arn, "/")+1:])).
		Set("path", p.get("Path")).
		Set("description", p.get("Description")).
		Set("policy", p.json("PolicyDocument"))
	bs = append(bs, r)
	ims = append(ims, importBlock("aws_iam_policy."+name, arn))
	if p.has("Users") || p.has("Groups") {
		c.fail("%s: attaching to users or groups is not supported, only Roles", p.where)
	}
	rolesRaw, _ := p.raw("Roles")
	rawList, _ := rolesRaw.([]any)
	for i, role := range p.items("Roles") {
		n := fmt.Sprintf("%s_%d", name, i)
		bs = append(bs, resource("aws_iam_role_policy_attachment", n).Set("role", role).Set("policy_arn", Raw("aws_iam_policy."+name+".arn")))
		ims = append(ims, importBlock("aws_iam_role_policy_attachment."+n, c.staticID(rawList[i], fmt.Sprintf("%s.Roles[%d]", p.where, i))+"/"+arn))
	}
	return bs, ims
}

// ---- Logs, SSM ----

func logGroup(c *conv, p *props, name string) (bs, ims []*Block) {
	n := p.physical()
	r := resource("aws_cloudwatch_log_group", name).Set("name", p.nameOr("LogGroupName", n)).
		Set("retention_in_days", p.get("RetentionInDays")).
		Set("kms_key_id", p.get("KmsKeyId")).
		Set("log_group_class", p.get("LogGroupClass")).
		Set("tags", p.tags("Tags"))
	return []*Block{r}, []*Block{importBlock("aws_cloudwatch_log_group."+name, n)}
}

func ssmParameter(c *conv, p *props, name string) (bs, ims []*Block) {
	n := p.physical()
	r := resource("aws_ssm_parameter", name).Set("name", p.nameOr("Name", n)).
		Set("type", p.get("Type")).
		Set("value", p.get("Value")).
		Set("description", p.get("Description")).
		Set("allowed_pattern", p.get("AllowedPattern")).
		Set("tier", p.get("Tier")).
		Set("data_type", p.get("DataType")).
		Set("tags", p.get("Tags")) // a plain map for this resource type
	return []*Block{r}, []*Block{importBlock("aws_ssm_parameter."+name, n)}
}

// ---- DynamoDB ----

func dynamoTable(c *conv, p *props, name string) (bs, ims []*Block) {
	n := p.physical()
	r := resource("aws_dynamodb_table", name).Set("name", p.nameOr("TableName", n))
	billing := p.get("BillingMode")
	if billing == nil {
		billing = "PROVISIONED" // CloudFormation's default
	}
	r.Set("billing_mode", billing)
	for _, k := range p.list("KeySchema") {
		attr := k.get("AttributeName")
		kt, _ := k.raw("KeyType")
		if kt == "HASH" {
			r.Set("hash_key", attr)
		} else {
			r.Set("range_key", attr)
		}
		k.done()
	}
	if t := p.sub("ProvisionedThroughput"); t != nil {
		r.Set("read_capacity", t.get("ReadCapacityUnits")).Set("write_capacity", t.get("WriteCapacityUnits"))
		t.done()
	}
	if s := p.sub("StreamSpecification"); s != nil {
		r.Set("stream_enabled", true).Set("stream_view_type", s.get("StreamViewType"))
		s.done()
	}
	r.Set("table_class", p.get("TableClass")).Set("deletion_protection_enabled", p.get("DeletionProtectionEnabled"))
	for _, a := range p.list("AttributeDefinitions") {
		r.Add("attribute").Set("name", a.get("AttributeName")).Set("type", a.get("AttributeType"))
		a.done()
	}
	for _, g := range p.list("GlobalSecondaryIndexes") {
		gb := r.Add("global_secondary_index").Set("name", g.get("IndexName"))
		for _, k := range g.list("KeySchema") {
			kt, _ := k.raw("KeyType")
			if kt == "HASH" {
				gb.Set("hash_key", k.get("AttributeName"))
			} else {
				gb.Set("range_key", k.get("AttributeName"))
			}
			k.done()
		}
		if pr := g.sub("Projection"); pr != nil {
			gb.Set("projection_type", pr.get("ProjectionType")).Set("non_key_attributes", pr.get("NonKeyAttributes"))
			pr.done()
		}
		if t := g.sub("ProvisionedThroughput"); t != nil {
			gb.Set("read_capacity", t.get("ReadCapacityUnits")).Set("write_capacity", t.get("WriteCapacityUnits"))
			t.done()
		}
		g.done()
	}
	if t := p.sub("TimeToLiveSpecification"); t != nil {
		r.Add("ttl").Set("attribute_name", t.get("AttributeName")).Set("enabled", t.get("Enabled"))
		t.done()
	}
	if t := p.sub("PointInTimeRecoverySpecification"); t != nil {
		r.Add("point_in_time_recovery").Set("enabled", t.get("PointInTimeRecoveryEnabled"))
		t.done()
	}
	if t := p.sub("SSESpecification"); t != nil {
		r.Add("server_side_encryption").Set("enabled", t.get("SSEEnabled"))
		t.done()
	}
	r.Set("tags", p.tags("Tags"))
	return []*Block{r}, []*Block{importBlock("aws_dynamodb_table."+name, n)}
}
