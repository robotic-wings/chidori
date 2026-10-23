//! Agent recipes: YAML-defined wrappers that bundle an agent file path with
//! default inputs, a cron schedule, and optional metadata. Recipes are the
//! unit the scheduler operates on.
//!
//! Example `recipes/daily-digest.yaml`:
//!
//! ```yaml
//! name: daily-digest
//! agent: agents/digest.ts
//! schedule: "0 9 * * *"        # every day at 09:00
//! inputs:
//!   channel: "#general"
//!   lookback_hours: 24
//! description: "Summarize yesterday's activity and post to Slack"
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// serde_yaml is used via its full path (`serde_yaml::from_str`) so no top-level
// import is needed.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipe {
    pub name: String,
    pub agent: PathBuf,
    /// Cron expression. Standard 5-field form.
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub inputs: Value,
    #[serde(default)]
    pub description: Option<String>,
}

impl Recipe {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading recipe {}", path.display()))?;
        // JSON and YAML both deserialize via serde; pick the parser from the
        // file extension. YAML is the documented format but JSON stays
        // supported for tooling that prefers it.
        let recipe: Recipe = if path.extension().and_then(|e| e.to_str()) == Some("json") {
            serde_json::from_str(&text)
                .with_context(|| format!("parsing recipe JSON {}", path.display()))?
        } else {
            serde_yaml::from_str(&text)
                .with_context(|| format!("parsing recipe YAML {}", path.display()))?
        };
        Ok(recipe)
    }

    pub fn load_dir(dir: &Path) -> Result<Vec<Recipe>> {
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if matches!(ext, "yaml" | "yml" | "json") {
                match Recipe::load(&path) {
                    Ok(r) => out.push(r),
                    Err(e) => tracing::warn!("recipe {}: {}", path.display(), e),
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_and_json_recipes_preserve_fields_and_cron() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = dir.path().join("digest.yaml");
        let json = dir.path().join("digest.json");
        std::fs::write(
            &yaml,
            "name: digest\nagent: agents/digest.ts\nschedule: '0 9 * * 1-5'\ninputs:\n  channel: '#general'\n",
        )
        .unwrap();
        std::fs::write(
            &json,
            r##"{"name":"digest-json","agent":"agents/digest.ts","schedule":"0 9 * * 1-5","inputs":{"channel":"#general"}}"##,
        )
        .unwrap();

        let yaml_recipe = Recipe::load(&yaml).unwrap();
        let json_recipe = Recipe::load(&json).unwrap();
        for recipe in [&yaml_recipe, &json_recipe] {
            assert_eq!(recipe.agent, PathBuf::from("agents/digest.ts"));
            assert_eq!(recipe.schedule.as_deref(), Some("0 9 * * 1-5"));
            assert_eq!(recipe.inputs["channel"], "#general");
        }
    }

    #[test]
    fn load_dir_keeps_yaml_yml_and_json_recipe_fields_and_cron() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("digest.yaml"),
            "name: digest-yaml\nagent: agents/digest.ts\nschedule: '0 9 * * 1-5'\ninputs: { channel: yaml }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("report.yml"),
            "name: report-yml\nagent: agents/report.ts\nschedule: '30 8 * * 1-5'\ninputs: { channel: yml }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("archive.json"),
            r#"{"name":"archive-json","agent":"agents/archive.ts","schedule":"0 0 * * 0","inputs":{"channel":"json"}}"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("ignored.txt"), "not a recipe").unwrap();

        let mut recipes = Recipe::load_dir(dir.path()).unwrap();
        recipes.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(recipes.len(), 3);
        let decoded = recipes
            .iter()
            .map(|recipe| {
                (
                    recipe.name.as_str(),
                    recipe.agent.as_path(),
                    recipe.schedule.as_deref(),
                    recipe.inputs["channel"].as_str(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![
                (
                    "archive-json",
                    Path::new("agents/archive.ts"),
                    Some("0 0 * * 0"),
                    Some("json"),
                ),
                (
                    "digest-yaml",
                    Path::new("agents/digest.ts"),
                    Some("0 9 * * 1-5"),
                    Some("yaml"),
                ),
                (
                    "report-yml",
                    Path::new("agents/report.ts"),
                    Some("30 8 * * 1-5"),
                    Some("yml"),
                ),
            ]
        );
    }
}
