using Tiingo_Julia
using Documenter

DocMeta.setdocmeta!(Tiingo_Julia, :DocTestSetup, :(using Tiingo_Julia); recursive = true)

const page_rename = Dict("developer.md" => "Developer docs") # Without the numbers
const numbered_pages = [
  file for
  file in readdir(joinpath(@__DIR__, "src")) if file != "index.md" && splitext(file)[2] == ".md"
]

makedocs(;
    modules = [Tiingo_Julia],
    authors = "Kojiroh <kojiroh.homma@gmail.com> and contributors",
    repo = "https://github.com/10kpw/Tiingo_Julia.jl/blob/{commit}{path}#{line}",
    sitename = "Tiingo_Julia.jl",
    format = Documenter.HTML(; canonical = "https://10kpw.github.io/Tiingo_Julia.jl"),
    pages = ["index.md"; numbered_pages],
)

deploydocs(; repo = "github.com/10kpw/Tiingo_Julia.jl")
