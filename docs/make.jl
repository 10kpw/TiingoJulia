using TiingoJulia
using Documenter

DocMeta.setdocmeta!(TiingoJulia, :DocTestSetup, :(using TiingoJulia); recursive = true)

const page_rename = Dict("developer.md" => "Developer docs") # Without the numbers
const numbered_pages = [
  file for
  file in readdir(joinpath(@__DIR__, "src")) if file != "index.md" && splitext(file)[2] == ".md"
]

makedocs(;
    modules = [TiingoJulia],
    authors = "Kojiroh <kojiroh.homma@gmail.com> and contributors",
    repo = "https://github.com/10kpw/TiingoJulia.jl/blob/{commit}{path}#{line}",
    sitename = "TiingoJulia.jl",
    format = Documenter.HTML(; canonical = "https://10kpw.github.io/TiingoJulia.jl"),
    pages = ["index.md"; numbered_pages],
)

deploydocs(; repo = "github.com/10kpw/TiingoJulia.jl")
