module DataStreamsIntegrationTests

using DataStreams, Base.Test

const DSTESTDIR = joinpath(dirname(@__FILE__), "../test")

export Tester, scalartransforms, vectortransforms, DSTESTDIR

type Tester
    name::String
    highlevel::Function
    hashighlevel::Bool
    constructor::DataType
    args::Tuple
    scalartransforms::Dict
    vectortransforms::Dict
    sinktodf::Function
    cleanup::Function
end

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
                 (a <: Integer && b <: Integer)

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

function teststream(sources, sinks)
    for source in sources
        for sink in sinks
            try
            if source.hashighlevel
            println("[$(now())]: Test high-level from source to sink; e.g. CSV.read")
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args")
            si = source.highlevel(source.args..., sink.constructor, sink.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + append")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + transforms")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + append + transforms")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name)")
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(source.args..., sinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + append")
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(source.args..., sinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + transforms")
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(source.args..., sinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + append + transforms`")
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(source.args..., sinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + append")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + transforms")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + append + transforms")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name)")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(soinst, sinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + append")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(soinst, sinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + transforms")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(soinst, sinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + append + transforms")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(soinst, sinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            end

            if sink.hashighlevel
            println("[$(now())]: Test high-level to sink from source; e.g. CSV.write")
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args")
            si = sink.highlevel(sink.args..., source.constructor, source.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + append")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + transforms")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + append + transforms")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args")
            sinst = sink.constructor(sink.args...)
            si = sink.highlevel(sinst, source.constructor, source.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + append")
            sinst = sink.constructor(sink.args...; append=true)
            si = sink.highlevel(sinst, source.constructor, source.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + transforms")
            sinst = sink.constructor(sink.args...)
            si = sink.highlevel(sinst, source.constructor, source.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + append + transforms`")
            sinst = sink.constructor(sink.args...; append=true)
            si = sink.highlevel(sinst, source.constructor, source.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name)")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + append")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + transforms")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + append + transforms")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name)")
            sinst = sink.constructor(sink.args...)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si))
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + append")
            sinst = sink.constructor(sink.args...; append=true)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + transforms")
            sinst = sink.constructor(sink.args...)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), false, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + append + transforms")
            sinst = sink.constructor(sink.args...; append=true)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), true, true)
            println("done")
            end
            catch e
                rethrow(e)
            finally
            println("[$(now())]: cleanup")
            sink.cleanup(sink.args...)
            end
        end
    end
end

end # module
