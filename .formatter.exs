# Formatting inputs. gen/ is excluded by omission: it is generated (and
# gitignored), exactly as sdk/go excludes gen/ from gofmt in the Justfile.
[
  inputs: ["{mix,lib,test}/**/*.{ex,exs}", "mix.exs", ".formatter.exs"],
  line_length: 100
]
