//! A template compiler, not a workflow scheduler: one template becomes one sequential sbatch script.
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read},
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub id: String,
    pub name: String,
    pub version: String,
    pub application: String,
    #[serde(default)]
    pub description: String,
    pub image: String,
    pub references: BTreeMap<String, String>,
    pub inputs: BTreeMap<String, InputSpec>,
    pub parameters: BTreeMap<String, ParameterSpec>,
    pub resources: Resources,
    pub steps: Vec<Step>,
    pub outputs: BTreeMap<String, OutputSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub kind: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterSpec {
    #[serde(rename = "type")]
    pub kind: String,
    pub default: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpus: u32,
    pub memory_gb: u32,
    pub time_minutes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub id: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    pub path: String,
    pub kind: String,
    #[serde(default = "default_true")]
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub script_path: PathBuf,
    pub snapshot: Value,
}

fn default_true() -> bool {
    true
}

pub fn load_templates(dir: &Path) -> Result<Vec<Template>> {
    let mut paths = fs::read_dir(dir)
        .context("read workflow template directory")?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut templates = Vec::new();
    for path in paths {
        if matches!(
            path.extension().and_then(|s| s.to_str()),
            Some("yaml" | "yml")
        ) {
            let template = parse_template(&fs::read_to_string(&path)?)
                .with_context(|| format!("invalid template {}", path.display()))?;
            if templates
                .iter()
                .any(|other: &Template| other.id == template.id)
            {
                bail!("duplicate workflow id {}", template.id);
            }
            templates.push(template);
        }
    }
    Ok(templates)
}

pub fn parse_template(yaml: &str) -> Result<Template> {
    if yaml.len() > 256 * 1024 {
        bail!("workflow template exceeds 256 KiB");
    }
    let template: Template = serde_yaml::from_str(yaml).context("invalid workflow YAML")?;
    validate_template(&template)?;
    Ok(template)
}

fn validate_template(template: &Template) -> Result<()> {
    key(&template.id)?;
    key(&template.version)?;
    key(&template.application)?;
    if template.name.trim().is_empty() || template.name.len() > 200 {
        bail!("template needs a name of 1..200 bytes");
    }
    if template.image.is_empty() || template.image.contains(['\0', '\n', '\r']) {
        bail!("invalid container image path");
    }
    if template.inputs.is_empty() || template.steps.is_empty() || template.outputs.is_empty() {
        bail!("template requires inputs, steps and outputs");
    }
    check_resources(&template.resources)?;
    for (name, input) in &template.inputs {
        key(name)?;
        if !input.required {
            bail!("MVP supports required inputs only");
        }
        if !matches!(input.kind.as_str(), "fastq" | "file") {
            bail!("unsupported input kind");
        }
    }
    for (name, reference) in &template.references {
        key(name)?;
        if reference.is_empty() || reference.contains(['\0', '\n', '\r']) {
            bail!("invalid reference path");
        }
    }
    for (name, parameter) in &template.parameters {
        key(name)?;
        if let Some(format) = &parameter.format {
            if format != "sample_id" {
                bail!("unsupported parameter format");
            }
        }
        if let (Some(minimum), Some(maximum)) = (parameter.minimum, parameter.maximum) {
            if minimum > maximum {
                bail!("parameter minimum exceeds maximum");
            }
        }
        validate_parameter(name, parameter, &parameter.default)?;
    }
    for (name, output) in &template.outputs {
        key(name)?;
        safe_relative(&output.path)?;
        if !matches!(output.kind.as_str(), "vcf" | "file") {
            bail!("unsupported output kind");
        }
    }
    let mut step_ids = std::collections::BTreeSet::new();
    for step in &template.steps {
        key(&step.id)?;
        if !step_ids.insert(&step.id) {
            bail!("duplicate workflow step id");
        }
        if step.command.is_empty() || step.command.len() > 64 * 1024 || step.command.contains('\0')
        {
            bail!("invalid workflow step command");
        }
        // Resolve a dummy map now so malformed/unknown placeholders fail before registration.
        let values = placeholder_values(template);
        render(&step.command, &values)?;
    }
    Ok(())
}

fn key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 100
        || value.starts_with('-')
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
    {
        bail!("invalid workflow identifier: {value:?}");
    }
    Ok(())
}

fn safe_relative(value: &str) -> Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains(['\\', ':', '\0', '\n', '\r'])
        || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        bail!("output paths must be relative paths without traversal");
    }
    Ok(())
}

fn check_resources(resources: &Resources) -> Result<()> {
    if !(1..=128).contains(&resources.cpus)
        || !(1..=1024).contains(&resources.memory_gb)
        || !(1..=10_080).contains(&resources.time_minutes)
    {
        bail!("resource limits: cpus 1..128, memory_gb 1..1024, time_minutes 1..10080");
    }
    Ok(())
}

fn validate_parameter(name: &str, spec: &ParameterSpec, value: &Value) -> Result<()> {
    let valid_type = match spec.kind.as_str() {
        "string" => value
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)),
        "integer" => value.is_i64() || value.is_u64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        _ => false,
    };
    if !valid_type {
        bail!("parameter {name} has invalid type or value");
    }
    if let Some(number) = value.as_f64() {
        if spec.minimum.is_some_and(|min| number < min)
            || spec.maximum.is_some_and(|max| number > max)
        {
            bail!("parameter {name} is outside its allowed range");
        }
    }
    if spec
        .allowed_values
        .as_ref()
        .is_some_and(|allowed| !allowed.contains(value))
    {
        bail!("parameter {name} is not an allowed value");
    }
    if spec.format.as_deref() == Some("sample_id") {
        let sample = value.as_str().context("sample_id must be a string")?;
        key(sample)?;
    }
    Ok(())
}

fn scalar(value: &Value) -> Result<String> {
    if let Some(text) = value.as_str() {
        Ok(text.to_owned())
    } else if value.is_number() || value.is_boolean() {
        Ok(value.to_string())
    } else {
        bail!("template values must be scalar");
    }
}

/// POSIX quoting. Template commands are trusted admin code; every caller-controlled value is one shell word.
fn shell_quote(value: &str) -> Result<String> {
    if value.contains('\0') {
        bail!("NUL is not allowed in shell arguments");
    }
    Ok(format!("'{}'", value.replace('\'', "'\"'\"'")))
}

fn placeholder_values(template: &Template) -> BTreeMap<String, String> {
    let mut values = BTreeMap::from([("run_dir".into(), String::new())]);
    for (prefix, names) in [
        ("input", template.inputs.keys().collect::<Vec<_>>()),
        ("param", template.parameters.keys().collect()),
        ("reference", template.references.keys().collect()),
        ("output", template.outputs.keys().collect()),
    ] {
        for name in names {
            values.insert(format!("{prefix}:{name}"), String::new());
        }
    }
    values
}

fn render(command: &str, values: &BTreeMap<String, String>) -> Result<String> {
    let mut rendered = String::new();
    let mut remaining = command;
    while let Some(start) = remaining.find("{{") {
        rendered.push_str(&remaining[..start]);
        let after = &remaining[start + 2..];
        let end = after.find("}}").context("unclosed workflow placeholder")?;
        let name = after[..end].trim();
        let value = values
            .get(name)
            .with_context(|| format!("unknown workflow placeholder {name}"))?;
        rendered.push_str(&shell_quote(value)?);
        remaining = &after[end + 2..];
    }
    if remaining.contains("}}") {
        bail!("unmatched workflow placeholder delimiter");
    }
    rendered.push_str(remaining);
    Ok(rendered)
}

fn checked_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("{label} does not exist: {}", path.display()))?;
    if !canonical.is_file() || fs::metadata(&canonical)?.len() == 0 {
        bail!("{label} must be a nonempty regular file");
    }
    Ok(canonical)
}

fn path_text(path: &Path) -> Result<String> {
    Ok(path.to_str().context("path is not UTF-8")?.to_owned())
}

pub fn output_paths(template: &Template, run_dir: &Path) -> Result<BTreeMap<String, PathBuf>> {
    template
        .outputs
        .iter()
        .map(|(name, output)| {
            safe_relative(&output.path)?;
            Ok((name.clone(), run_dir.join(&output.path)))
        })
        .collect()
}

pub fn deploy(
    template: &Template,
    inputs: &Value,
    parameters: &Value,
    run_dir: &Path,
    partition: &str,
    mock: bool,
) -> Result<Deployment> {
    validate_template(template)?;
    crate::slurm::validate_identifier(partition)?;
    let input_values = inputs.as_object().context("inputs must be a JSON object")?;
    let param_values = parameters
        .as_object()
        .context("parameters must be a JSON object")?;
    for name in input_values.keys() {
        if !template.inputs.contains_key(name) {
            bail!("unknown input {name}");
        }
    }
    for name in param_values.keys() {
        if !template.parameters.contains_key(name) {
            bail!("unknown parameter {name}");
        }
    }
    let mut values = placeholder_values(template);
    let mut resolved_inputs = BTreeMap::new();
    let mut mounts = BTreeMap::new();
    for name in template.inputs.keys() {
        let input = input_values
            .get(name)
            .and_then(Value::as_str)
            .with_context(|| format!("missing input {name}"))?;
        let input_path = checked_file(Path::new(input), &format!("input {name}"))?;
        let input_text = path_text(&input_path)?;
        values.insert(format!("input:{name}"), input_text.clone());
        resolved_inputs.insert(name.clone(), input_text);
        mounts.insert(
            input_path
                .parent()
                .context("input has no parent")?
                .to_owned(),
            "ro",
        );
    }
    let mut resolved_parameters = BTreeMap::new();
    for (name, spec) in &template.parameters {
        let value = param_values.get(name).unwrap_or(&spec.default);
        validate_parameter(name, spec, value)?;
        values.insert(format!("param:{name}"), scalar(value)?);
        resolved_parameters.insert(name.clone(), value.clone());
    }
    if let (Some(tumor), Some(normal)) = (
        resolved_parameters.get("tumor_sample"),
        resolved_parameters.get("normal_sample"),
    ) {
        if tumor == normal {
            bail!("tumor_sample and normal_sample must differ");
        }
    }
    let resource = |name: &str, fallback: u32| -> Result<u32> {
        match resolved_parameters.get(name) {
            Some(value) => value
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .with_context(|| format!("{name} must be a positive integer")),
            None => Ok(fallback),
        }
    };
    let resources = Resources {
        cpus: resource("threads", template.resources.cpus)?,
        memory_gb: resource("memory_gb", template.resources.memory_gb)?,
        time_minutes: resource("time_minutes", template.resources.time_minutes)?,
    };
    check_resources(&resources)?;
    let image = if mock {
        template.image.clone()
    } else {
        path_text(&checked_file(
            Path::new(&template.image),
            "Apptainer SIF image",
        )?)?
    };
    let mut resolved_references = BTreeMap::new();
    for (name, reference) in &template.references {
        let reference = if mock {
            reference.clone()
        } else {
            let reference_path = checked_file(Path::new(reference), &format!("reference {name}"))?;
            mounts.insert(
                reference_path
                    .parent()
                    .context("reference has no parent")?
                    .to_owned(),
                "ro",
            );
            path_text(&reference_path)?
        };
        values.insert(format!("reference:{name}"), reference.clone());
        resolved_references.insert(name.clone(), reference);
    }
    fs::create_dir_all(run_dir).context("create workflow run directory")?;
    let run_dir = fs::canonicalize(run_dir)?;
    let run_text = path_text(&run_dir)?;
    // Slurm #SBATCH directives are parsed before the shell: reject paths needing directive quoting.
    if !mock
        && run_text
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '\'' | '"' | '\\' | '#' | ':'))
    {
        bail!("real Slurm run directory must not contain whitespace or directive metacharacters");
    }
    values.insert("run_dir".into(), run_text.clone());
    mounts.insert(run_dir.clone(), "rw");
    for (name, output) in output_paths(template, &run_dir)? {
        fs::create_dir_all(output.parent().context("output has no parent")?)?;
        values.insert(format!("output:{name}"), path_text(&output)?);
    }
    let bind_args = mounts
        .into_iter()
        .map(|(path, mode)| {
            let text = path_text(&path)?;
            if !mock && text.contains([',', ':', '\n', '\r']) {
                bail!("Apptainer bind paths cannot contain colon, comma or newlines");
            }
            Ok(format!(
                " --bind {}",
                shell_quote(&format!("{text}:{text}:{mode}"))?
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join("");
    let mut script = format!("#!/usr/bin/env bash\n#SBATCH --job-name={}\n#SBATCH --partition={}\n#SBATCH --cpus-per-task={}\n#SBATCH --mem={}G\n#SBATCH --time={}\n#SBATCH --chdir={}\n#SBATCH --output={}/stdout.log\n#SBATCH --error={}/stderr.log\nset -euo pipefail\numask 077\ncd {}\n", template.id, partition, resources.cpus, resources.memory_gb, resources.time_minutes, run_text, run_text, run_text, shell_quote(&run_text)?);
    script.push_str("exec > >(tee -a run.log) 2> >(tee -a run.log >&2)\ntrap 'code=$?; printf \"%s\\n\" \"$code\" > exit_code.txt; date -u +%FT%TZ > finished_at.txt' EXIT\ndate -u +%FT%TZ > started_at.txt\n");
    if mock {
        script.push_str("# SIMULATED: this script is an inspection artifact; no Slurm or genomics program is executed.\n");
    }
    for step in &template.steps {
        let command = render(&step.command, &values)?;
        script.push_str(&format!(
            "\nprintf '%s\\n' {}\napptainer exec --cleanenv{} {} bash -lc {}\n",
            shell_quote(&format!("step={}", step.id))?,
            bind_args,
            shell_quote(&image)?,
            shell_quote(&format!("set -euo pipefail; {command}"))?
        ));
    }
    let script_path = run_dir.join("job.sbatch");
    fs::write(&script_path, script)?;
    let template_json = serde_json::to_vec(template)?;
    let template_sha256 = format!("{:x}", Sha256::digest(&template_json));
    let snapshot = json!({
        "template": template, "template_sha256": template_sha256,
        "inputs": resolved_inputs, "parameters": resolved_parameters,
        "references": resolved_references, "image": image, "resources": resources,
        "partition": partition, "run_dir": run_text,
        "deployed_at": chrono::Utc::now().to_rfc3339(), "simulated": mock,
        "checks": {"inputs": "passed", "image": if mock {"skipped_simulation"} else {"passed"},
            "references": if mock {"skipped_simulation"} else {"passed"}, "script": "generated"}
    });
    fs::write(
        run_dir.join("deployment.json"),
        serde_json::to_vec_pretty(&snapshot)?,
    )?;
    Ok(Deployment {
        script_path,
        snapshot,
    })
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn inspect_vcf(path: &Path) -> Result<Value> {
    let mut fileformat = false;
    let mut columns = false;
    let mut variants = 0_u64;
    for line in BufReader::new(fs::File::open(path)?).lines() {
        let line = line?;
        if line.starts_with("##fileformat=VCFv4.") {
            fileformat = true;
        } else if line.starts_with("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO") {
            columns = true;
        } else if !line.starts_with('#') && !line.trim().is_empty() {
            let fields: Vec<_> = line.split('\t').collect();
            if !columns
                || fields.len() < 8
                || fields[0].is_empty()
                || fields[1].parse::<u64>().ok().is_none_or(|n| n == 0)
                || fields[3].is_empty()
                || fields[4].is_empty()
            {
                bail!("malformed VCF data record");
            }
            variants += 1;
        }
    }
    if !fileformat || !columns {
        bail!("VCF is missing required fileformat or column headers");
    }
    Ok(json!({"format": "VCF", "variant_records": variants, "structural_validation": "passed"}))
}

/// Called once after a mock job completes. A durable marker prevents later validations from repairing
/// deleted/corrupted outputs and accidentally turning a failed check into a success.
pub fn materialize_mock_outputs(template: &Template, run_dir: &Path) -> Result<()> {
    validate_template(template)?;
    let run_dir = fs::canonicalize(run_dir).context("run directory does not exist")?;
    let marker = run_dir.join("mock-fixtures-created.json");
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(marker)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(b"{\"simulated\":true,\"biological_computation_performed\":false}\n")?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    for output in template.outputs.values() {
        let path = run_dir.join(&output.path);
        let parent = path.parent().context("output has no parent")?;
        fs::create_dir_all(parent)?;
        if !fs::canonicalize(parent)?.starts_with(&run_dir) {
            bail!("mock output escaped run directory");
        }
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        use std::io::Write;
        if output.kind == "vcf" {
            file.write_all(b"##fileformat=VCFv4.2\n##source=HPC-Gateway-SIMULATED-FIXTURE\n##contig=<ID=chr1>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\nchr1\t100\t.\tA\tG\t60\tPASS\t.\n")?;
        } else {
            file.write_all(b"SIMULATED FIXTURE: no biological computation performed\n")?;
        }
    }
    for name in ["stdout.log", "run.log"] {
        fs::write(
            run_dir.join(name),
            "SIMULATED: deterministic fixtures generated; no genomics tools were executed.\n",
        )?;
    }
    fs::write(run_dir.join("stderr.log"), "")?;
    fs::write(run_dir.join("exit_code.txt"), "0\n")?;
    Ok(())
}

/// Checks output existence and basic VCF structure. This is technical validation, not clinical accuracy.
/// The caller must first confirm that the associated job completed with exit code 0:0.
pub fn validate_results(template: &Template, run_dir: &Path, mock: bool) -> Result<Value> {
    validate_template(template)?;
    let run_dir = fs::canonicalize(run_dir).context("run directory does not exist")?;
    let mut results = Vec::new();
    let mut passed = true;
    for (name, output) in &template.outputs {
        let path = run_dir.join(&output.path);
        if !path.exists() {
            if output.required {
                passed = false;
            }
            results.push(json!({"name": name, "path": output.path, "required": output.required, "passed": !output.required, "error": "missing output"}));
            continue;
        }
        let inspected = (|| -> Result<Value> {
            let canonical = fs::canonicalize(&path)?;
            if !canonical.starts_with(&run_dir)
                || fs::symlink_metadata(&path)?.file_type().is_symlink()
            {
                bail!("result path escaped the run directory or is a symlink");
            }
            let size = fs::metadata(&canonical)?.len();
            if !canonical.is_file() || size == 0 {
                bail!("output must be a nonempty regular file");
            }
            let validation = if output.kind == "vcf" {
                inspect_vcf(&canonical)?
            } else {
                json!({"structural_validation":"passed"})
            };
            Ok(
                json!({"name": name, "path": output.path, "size_bytes": size,
                "sha256": file_sha256(&canonical)?, "passed": true, "details": validation}),
            )
        })();
        match inspected {
            Ok(result) => results.push(result),
            Err(error) => {
                passed = false;
                results.push(json!({"name":name,"path":output.path,"passed":false,"error":error.to_string()}));
            }
        }
    }
    let report = json!({
        "workflow_id": template.id, "workflow_version": template.version,
        "generated_at": chrono::Utc::now().to_rfc3339(), "simulated": mock,
        "validation_scope": "output existence, SHA-256 integrity and basic VCF structure; biological accuracy is not evaluated",
        "passed": passed, "results": results,
        "biological_validation": {"performed": false, "reason": "requires independently curated truth sets, comparison tools and acceptance thresholds"}
    });
    fs::write(
        run_dir.join("validation-report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("hpc-workflow-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn germline() -> Template {
        parse_template(include_str!("../workflows/germline.yaml")).unwrap()
    }
    #[test]
    fn quotes_all_user_values_as_single_words() {
        let rendered = render(
            "tool --sample {{param:sample}}",
            &BTreeMap::from([("param:sample".into(), "x'; touch /tmp/pwn; #".into())]),
        )
        .unwrap();
        assert_eq!(rendered, "tool --sample 'x'\"'\"'; touch /tmp/pwn; #'");
        assert!(render("tool {{unknown}}", &BTreeMap::new()).is_err());
    }
    #[test]
    fn missing_input_is_rejected_even_in_mock_mode() {
        let directory = Temp::new();
        let error = deploy(
            &germline(),
            &json!({"reads_r1":directory.0.join("absent").to_string_lossy(),"reads_r2":"absent"}),
            &json!({}),
            &directory.0.join("run"),
            "debug",
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not exist"));
    }
    #[test]
    fn real_deployment_checks_image_and_reference_files() {
        let directory = Temp::new();
        let input = directory.0.join("reads.fastq");
        fs::write(&input, "@r\nACGT\n+\nFFFF\n").unwrap();
        let inputs = json!({"reads_r1":input.to_string_lossy(),"reads_r2":input.to_string_lossy()});
        let mut template = germline();
        template.image = directory
            .0
            .join("missing.sif")
            .to_string_lossy()
            .into_owned();
        let error = deploy(
            &template,
            &inputs,
            &json!({}),
            &directory.0.join("run"),
            "debug",
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("Apptainer SIF image"));
        let image = directory.0.join("fixture.sif");
        fs::write(
            &image,
            "nonempty placeholder used only for file-existence test",
        )
        .unwrap();
        template.image = image.to_string_lossy().into_owned();
        let error = deploy(
            &template,
            &inputs,
            &json!({}),
            &directory.0.join("run"),
            "debug",
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("reference"));
    }
    #[test]
    fn fails_invalid_vcf_and_preserves_failure_report() {
        let directory = Temp::new();
        let template = germline();
        let output = directory.0.join(&template.outputs["variants"].path);
        fs::create_dir_all(output.parent().unwrap()).unwrap();
        fs::write(output, "not a VCF\n").unwrap();
        let report = validate_results(&template, &directory.0, false).unwrap();
        assert_eq!(report["passed"], false);
        assert_eq!(report["biological_validation"]["performed"], false);
        assert!(directory.0.join("validation-report.json").is_file());
    }
    #[test]
    fn deploy_records_defaults_and_blocks_resource_and_partition_injection() {
        let directory = Temp::new();
        let input = directory.0.join("reads.fastq");
        fs::write(&input, "@r\nACGT\n+\nFFFF\n").unwrap();
        let inputs = json!({"reads_r1":input.to_string_lossy(),"reads_r2":input.to_string_lossy()});
        let template = germline();
        assert!(deploy(
            &template,
            &inputs,
            &json!({"threads":999}),
            &directory.0.join("bad"),
            "debug",
            true
        )
        .is_err());
        assert!(deploy(
            &template,
            &inputs,
            &json!({}),
            &directory.0.join("bad"),
            "debug\n#SBATCH --exclusive",
            true
        )
        .is_err());
        assert!(deploy(
            &template,
            &inputs,
            &json!({"shell":"evil"}),
            &directory.0.join("bad"),
            "debug",
            true
        )
        .is_err());
        let deployment = deploy(
            &template,
            &inputs,
            &json!({}),
            &directory.0.join("run"),
            "debug",
            true,
        )
        .unwrap();
        assert!(deployment.script_path.is_file());
        assert_eq!(deployment.snapshot["parameters"]["threads"], 4);
        assert_eq!(deployment.snapshot["simulated"], true);
        materialize_mock_outputs(&template, deployment.script_path.parent().unwrap()).unwrap();
        let report =
            validate_results(&template, deployment.script_path.parent().unwrap(), true).unwrap();
        assert_eq!(report["passed"], true);
        assert_eq!(report["simulated"], true);
        fs::remove_file(
            deployment
                .script_path
                .parent()
                .unwrap()
                .join(&template.outputs["variants"].path),
        )
        .unwrap();
        materialize_mock_outputs(&template, deployment.script_path.parent().unwrap()).unwrap();
        assert_eq!(
            validate_results(&template, deployment.script_path.parent().unwrap(), true).unwrap()
                ["passed"],
            false
        );
    }
    #[test]
    fn bundled_templates_and_output_paths_are_valid() {
        assert_eq!(germline().id, "germline");
        assert_eq!(
            parse_template(include_str!("../workflows/somatic.yaml"))
                .unwrap()
                .id,
            "somatic"
        );
        assert!(safe_relative("../secret").is_err());
        assert!(safe_relative("/etc/passwd").is_err());
        assert!(safe_relative("C:\\secret").is_err());
    }
}
