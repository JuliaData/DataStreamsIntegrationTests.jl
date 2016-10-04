module DataStreamsIntegrationTests

export Tester, scalartransforms, vectortransforms

type Tester
    name::String
    highlevel::Function
    constructor::DataType
    args::Tuple
    scalartransforms::Dict
    vectortransforms::Dict
    sinktodf::Function
    cleanup::Function
end

installed = Pkg.installed()

haskey(installed, "DataStreams") || Pkg.clone("DataStreams")
using DataStreams

PKGS = ["DataFrames", "CSV", "SQLite", "Feather", "ODBC"]

for pkg in PKGS
    haskey(installed, pkg) || Pkg.clone(pkg)
end

using Base.Test, DataStreams, DataFrames, CSV, SQLite, Feather, ODBC

# transforms
incr{T}(x::T) = x + 1
incr{T}(x::Nullable{T}) = isnull(x) ? Nullable{T}() : Nullable{T}(incr(get(x)))

doe_ify{T}(x::T) = string(x, " Doe")
doe_ify{T}(x::Nullable{T}) = isnull(x) ? Nullable{String}() : Nullable{String}(doe_ify(get(x)))

getlength(x) = length(x)
getlength(x::Nullable) = isnull(x) ? Nullable{Int}() : Nullable{Int}(getlength(get(x)))

div2{T}(x::T) = x / 2
div2{T}(x::Nullable{T}) = isnull(x) ? Nullable{T}() : Nullable{T}(div2(get(x)))

scalartransforms = Dict{String, Function}("id"=> incr, "firstname"=> doe_ify, "lastname"=> getlength, "salary"=> div2)
vectortransforms = Dict{String, Function}("id"=> x->[incr(i) for i in x], "firstname"=> x->[doe_ify(i) for i in x], "lastname"=> x->[getlength(i) for i in x], "salary"=> x->[div2(i) for i in x])

function gettransforms(source, sink)
    streamtypes = Data.streamtypes(sink.constructor)
    for typ in streamtypes
        if Data.streamtype(source.constructor, typ)
            return typ == Data.Column ? source.vectortransforms : source.scalartransforms
        end
    end
end

# tests
typequal{T}(::Type{T}, ::Type{T}) = true
typequal{T,S}(::Type{Nullable{T}}, ::Type{Nullable{S}}) = typequal(T, S)
typequal{T,S}(::Type{Nullable{T}}, ::Type{S}) = typequal(T, S)
typequal{T,S}(::Type{T}, ::Type{Nullable{S}}) = typequal(T, S)
typequal(a, b) = (a <: AbstractString && b <: AbstractString) ||
                 (a <: Integer && b <: Integer) ||
                 (a == ODBC.API.SQLDate && b == Date) ||
                 (a == Date && b == ODBC.API.SQLDate) ||
                 (a == ODBC.API.SQLTimestamp && b == DateTime) ||
                 (a == DateTime && b == ODBC.API.SQLTimestamp)

testnull{T}(v1::T, v2::T) = v1 == v2
testnull{T}(v1::Nullable{T}, v2::Nullable{T}) = (isnull(v1) && isnull(v2)) || (!isnull(v1) && !isnull(v2) && get(v1) == get(v2))
testnull{T}(v1::T, v2::Nullable{T}) = !isnull(v2) && get(v2) == v1
testnull{T}(v1::Nullable{T}, v2::T) = !isnull(v1) && get(v1) == v2
testnull{T, S}(v1::T, v2::Nullable{S}) = !isnull(v2) && get(v2) == v1
testnull{T, S}(v1::Nullable{T}, v2::S) = !isnull(v1) && get(v1) == v2
testnull{T, S}(v1::Nullable{T}, v2::Nullable{S}) = (isnull(v1) && isnull(v2)) || (!isnull(v1) && !isnull(v2) && get(v1) == get(v2))
testnull{T, S}(v1::T, v2::S) = v1 == v2

function check(df, appended=false, transformed=false)
    # test size
    cols, rows = 7, appended ? 140000 : 70000
    @test size(df) == (rows,cols)

    # test types
    expected_types = transformed ? [Int, String, Int, Float64, Float64, Date, DateTime] : [Int, String, String, Float64, Float64, Date, DateTime]
    types = Data.types(df, Data.Field)
    @test all([typequal(types[i], expected_types[i]) for i = 1:length(types)])

    # test values
    if transformed
        @test testnull(df[1, 1], 2)
        @test testnull(df[1, 2], "Lawrence Doe")
        @test testnull(df[1, 3], length("Powell"))
        @test isapprox(typeof(df[1, 4]) <: Nullable ? get(df[1, 4]) : df[1, 4], 87216.8 / 2; atol=0.01)
        @test isapprox(typeof(df[1, 5]) <: Nullable ? get(df[1, 5]) : df[1, 5], 26.47; atol=0.01)
        @test testnull(df[1, 6], Date(2002, 4, 9))
        @test testnull(df[1, 7], DateTime(2002, 1, 17, 21, 32, 0))

        @test testnull(df[end, 1], 70001)
        @test testnull(df[end, 2], "Craig Doe")
        @test testnull(df[end, 3], length("Robertson"))
        @test isnull(df[end, 4])
        @test isnull(df[end, 5])
        @test testnull(df[end, 6], Date(2008, 6, 23))
        @test testnull(df[end, 7], DateTime(2005, 4, 18, 7, 2, 0))
    else
        @test testnull(df[1, 1], 1)
        @test testnull(df[1, 2], "Lawrence")
        @test testnull(df[1, 3], "Powell")
        @test isapprox(typeof(df[1, 4]) <: Nullable ? get(df[1, 4]) : df[1, 4], 87216.8; atol=0.01)
        @test isapprox(typeof(df[1, 5]) <: Nullable ? get(df[1, 5]) : df[1, 5], 26.47; atol=0.01)
        @test testnull(df[1, 6], Date(2002, 4, 9))
        @test testnull(df[1, 7], DateTime(2002, 1, 17, 21, 32, 0))

        @test testnull(df[end, 1], 70000)
        @test testnull(df[end, 2], "Craig")
        @test testnull(df[end, 3], "Robertson")
        @test isnull(df[end, 4])
        @test isnull(df[end, 5])
        @test testnull(df[end, 6], Date(2008, 6, 23))
        @test testnull(df[end, 7], DateTime(2005, 4, 18, 7, 2, 0))
    end
end

end # module
