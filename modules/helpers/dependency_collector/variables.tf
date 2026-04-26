variable "dependencies" {
  description = "List of dependencies to be collected"
  type = list(object({
    name              = string
    included_patterns = list(string)
    excluded_patterns = list(string)
    source_path       = string
    target_sub_dir    = optional(string, null)
  }))
}


