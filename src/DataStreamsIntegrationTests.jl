module DataStreamsIntegrationTests

using DataStreams, Nulls, Base.Test

const DSTESTDIR = joinpath(dirname(dirname(@__FILE__)), "test")

export Tester, scalartransforms, vectortransforms, DSTESTDIR

type Tester
    name::String
    highlevel::Function
    hashighlevel::Bool
    constructor::Type
    args::Tuple
    scalartransforms::Dict
    vectortransforms::Dict
    sinktodf::Function
    cleanup::Function
end

# transforms
incr(x) = x + 1
incr(::Null) = null

doe_ify(x) = string(x, " Doe")
doe_ify(::Null) = null

getlength(x) = length(x)
getlength(x::Null) = null

div2(x) = x / 2
div2(x::Null) = null

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
typequal(::Type{T}, ::Type{T}) where {T} = true
typequal(::Type{Union{T, Null}}, ::Type{Union{S, Null}}) where {T,S} = typequal(T, S)
typequal(::Type{Union{T, Null}}, ::Type{S}) where {T,S} = typequal(T, S)
typequal(a, b) = (a <: ?AbstractString && b <: ?AbstractString) ||
                 (a <: ?Integer && b <: ?Integer) ||
                 (a <: ?Dates.TimeType && b <: ?Dates.TimeType)

function check(df, rows, appended=false, transformed=false)
    # test size
    cols, rows = 7, appended ? rows * 2 : rows
    @test size(Data.schema(df)) == (rows, cols)

    # test types
    expected_types = transformed ? [Int, String, Int, Float64, Float64, Date, DateTime] : [Int, String, String, Float64, Float64, Date, DateTime]
    types = Data.types(Data.schema(df))
    @test all([DataStreamsIntegrationTests.typequal(types[i], expected_types[i]) for i = 1:length(types)])

    # test values
    if transformed
        @test df[1, 1] == 2
        @test df[1, 2] == "Lawrence Doe"
        @test df[1, 3] == length("Powell")
        @test isapprox(df[1, 4], 87216.8 / 2; atol=0.01)
        @test isapprox(df[1, 5], 26.47; atol=0.01)
        @test df[1, 6] == Date(2002, 4, 9)
        @test df[1, 7] == DateTime(2002, 1, 17, 21, 32, 0)

        @test df[end, 1] == 70001
        @test df[end, 2] == "Craig Doe"
        @test df[end, 3] == length("Robertson")
        @test isapprox(df[end, 4], 33699.215; atol=0.01)
        @test isapprox(df[end, 5], 23.99; atol=0.01)
        @test df[end, 6] == Date(2008, 6, 23)
        @test df[end, 7] == DateTime(2005, 4, 18, 7, 2, 0)
    else
        @test df[1, 1] == 1
        @test df[1, 2] == "Lawrence"
        @test df[1, 3] == "Powell"
        @test isapprox(df[1, 4], 87216.8; atol=0.01)
        @test isapprox(df[1, 5], 26.47; atol=0.01)
        @test df[1, 6] == Date(2002, 4, 9)
        @test df[1, 7] == DateTime(2002, 1, 17, 21, 32, 0)

        @test df[end, 1] == 70000
        @test df[end, 2] == "Craig"
        @test df[end, 3] == "Robertson"
        @test isapprox(df[end, 4], 67398.43; atol=0.01)
        @test isapprox(df[end, 5], 23.99; atol=0.01)
        @test df[end, 6] == Date(2008, 6, 23)
        @test df[end, 7] == DateTime(2005, 4, 18, 7, 2, 0)
    end
end

source = csvsource
sink = dfsink

function teststream(sources, sinks; rows=70000)
    for source in sources
        for sink in sinks
            try
            if source.hashighlevel
            println("[$(now())]: Test high-level from source to sink; e.g. CSV.read")
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args")
            si = source.highlevel(source.args..., sink.constructor, sink.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + append")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + transforms")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) args + append + transforms")
            si = source.highlevel(source.args..., sink.constructor, sink.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name)")
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(source.args..., sinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + append")
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(source.args..., sinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + transforms")
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(source.args..., sinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Source: $(source.name) args => Sink: $(sink.name) + append + transforms`")
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(source.args..., sinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + append")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + transforms")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) args + append + transforms")
            soinst = source.constructor(source.args...)
            si = source.highlevel(soinst, sink.constructor, sink.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name)")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(soinst, sinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + append")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(soinst, sinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + transforms")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...)
            si = source.highlevel(soinst, sinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Source: $(source.name) => Sink: $(sink.name) + append + transforms")
            soinst = source.constructor(source.args...)
            sinst = sink.constructor(sink.args...; append=true)
            si = source.highlevel(soinst, sinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            end

            if sink.hashighlevel
            println("[$(now())]: Test high-level to sink from source; e.g. CSV.write")
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args")
            si = sink.highlevel(sink.args..., source.constructor, source.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + append")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + transforms")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) args + append + transforms")
            si = sink.highlevel(sink.args..., source.constructor, source.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args")
            sinst = sink.constructor(sink.args...)
            si = sink.highlevel(sinst, source.constructor, source.args...)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + append")
            sinst = sink.constructor(sink.args...; append=true)
            si = sink.highlevel(sinst, source.constructor, source.args...; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + transforms")
            sinst = sink.constructor(sink.args...)
            si = sink.highlevel(sinst, source.constructor, source.args...; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) args + append + transforms")
            sinst = sink.constructor(sink.args...; append=true)
            si = sink.highlevel(sinst, source.constructor, source.args...; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name)")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + append")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + transforms")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Sink: $(sink.name) args => Source: $(source.name) + append + transforms")
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sink.args..., soinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name)")
            sinst = sink.constructor(sink.args...)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + append")
            sinst = sink.constructor(sink.args...; append=true)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; append=true)
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + transforms")
            sinst = sink.constructor(sink.args...)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, false, true)
            println("[$(now())]: Sink: $(sink.name) => Source: $(source.name) + append + transforms")
            sinst = sink.constructor(sink.args...; append=true)
            soinst = source.constructor(source.args...)
            si = sink.highlevel(sinst, soinst; append=true, transforms=DataStreamsIntegrationTests.gettransforms(source, sink))
            DataStreamsIntegrationTests.check(sink.sinktodf(si), rows, true, true)
            println("[$(now())]: finished...")
            end
            catch e
                rethrow(e)
            finally
            sink.cleanup(sink.args...)
            end
        end
    end
end

end # module
