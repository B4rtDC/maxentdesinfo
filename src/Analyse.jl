module Analyse
    using CSV
    using DataFrames
    using Distributions
    using JSON3
    using LightGraphs, MetaGraphs
    using Plots
    using Plots.PlotMeasures
    using PyCall
    using SparseArrays
    

    import Dates: now, format
    import StatsBase: countmap
    """
        DTG()

    return DTG string for logging
    """
    function DTG()
        return "$(format(now(), "HH:MM:SS"))"
    end

    """
        loaddata(userfile::Union{String,Array{String,1}}, msgfiles::Union{String,Array{String,1}})

    From a (list of) user and message CSV file(s), get the dataframes. Works on the raw files downloaded from the Twitter misinformation report.

    ### kwargs
    * :usrfields : `Array{Symbol,1}` - the columns of the user dataframe you want to use (default: all)
    * :msgfields : `Array{Symbol,1}` - the columns of the message dataframe you want to use (default: all)

    ### Examples
    ```Julia
    loaddata("/path/to/files/userfile.csv", 
                ["/path/to/files/messagefile_1.csv", 
                    "/path/to/files/messagefile_1.csv"])

    ```

    ### Notes
    * Makes use of [`DataFrame!`](https://juliadata.github.io/DataFrames.jl/stable/lib/functions/#DataFrames.DataFrame!) 
    combined with [`CSV.File`](https://juliadata.github.io/CSV.jl/stable/#CSV.File) with a selection of columns for increased performance.
    Column types can also be passed for an additional speedup (see also [`CSV.File`](https://juliadata.github.io/CSV.jl/stable/#CSV.File) kwargs)
    * If multiple treads are available, they will be used to read the CSV files in parallel.

    See also: ['dropdataframes!'](@ref), ['setgraph!'](@ref), ['describe!'](@ref)
    """
    function loaddata(userfiles::Array{String,1}, msgfiles::Array{String,1}; kwargs...)
        @debug "$(DTG()) - loading desinfo data files"
        # default settings
        defusrfilter = [:userid, :user_screen_name]
        defmsgfilter = [:tweetid, :userid, :is_retweet, :retweet_tweetid, :retweet_userid, :in_reply_to_tweetid, :in_reply_to_userid]

        # Load up CSV data with only selected columns
        usrdf = vcat([DataFrame(CSV.File(f; select=get(kwargs,:usrfields, defusrfilter), 
                                            limit=get(kwargs,:limit, nothing))) 
                        for f in userfiles]...)
        msgdf = vcat([DataFrame(CSV.File(f; select=get(kwargs,:msgfields, defmsgfilter), 
                                            limit=get(kwargs,:limit, nothing),
                                            truestrings=["true", "True"], falsestrings=["false", "False"], # added for stablity
                                            types=Dict(:is_retweet => Bool)))
                        for f in msgfiles ]...)
        @debug "$(DTG()) - finished desinfo loading data files"
        return usrdf, msgdf
    end

    # additional methods for flexibility
    loaddata(userfile::String, msfgile::String; kwargs...) = loaddata([userfile], [msfgile]; kwargs...)
    loaddata(userfile::String, msfgiles::Array{String,1}; kwargs...) = loaddata([userfile], msfgiles; kwargs...)
    loaddata(userfiles::Array{String,1}, msfgile::String; kwargs...) = loaddata(userfiles, [msfgile]; kwargs...)

    """
        loaddata(path::String; kwargs...)

    Load data files from a path containing user and message CSV files.

    ### kwargs:
    * `:usrdata` : `String` - pattern used to recognize user data file(s) (default: "users_csv_hashed")
    * `:msgdata` : `String` - pattern used to recognize message data file(s) (default: "tweets_csv_hashed")
    * `:usrfields` : `Array{Symbol,1}` - the fields of the user dataframe you want to use (default: all)
    * `:msgfields` : `Array{Symbol,1}` - the fields of the message dataframe you want to use (default: all)
    * `:usrfilter` : `Array{String,1}` - an additional pattern that should found in the message data files (e.g. "_2020", default:[""])
    * `:msgfilter` : `Array{String,1}` - an additional pattern that should found in the message data files (e.g. "_2020", default:[""])

    See also: ['dropdataframes!'](@ref), ['setgraph!'](@ref), ['describe!'](@ref)

    """
    function loaddata(path::String; kwargs...)
        usrfiles = joinpath.(path, filter(x -> occursin(get(kwargs, :usrdata, "users_csv_hashed"), x) && 
                                                    !occursin(".jsonl",x) &&
                                                any([occursin(t,x) for t in get(kwargs, :usrfilter, [""]) ]), 
                                readdir(path)))
        msgfiles = joinpath.(path, filter(x -> occursin(get(kwargs, :usrdata, "tweets_csv_hashed"),x) && 
                                                    !occursin(".jsonl",x) &&
                                                any([occursin(t,x) for t in get(kwargs, :msgfilter, [""]) ]), 
                                readdir(path)))
        @warn usrfiles, msgfiles
        return loaddata(usrfiles, msgfiles; kwargs...)
    end

    """
        get_externals(path::String)

    From a twitter misinformation report path, extract and store the data 
    that still needs to be recovered (retweets/replies) using an additional tool
    in order to be able to generate an interaction network.
    """
    function get_externals(path::String)
        # export path
        fname = filter!(x->length(x)>0,split(path,"/"))[end]
        # load data
        @debug "$(DTG()) - loading data"
        udf,mdf = loaddata(path, 
                            usrfields=[:userid, :user_screen_name],
                            msgfields=[:tweetid, :userid, :is_retweet, :retweet_userid, 
                                       :retweet_tweetid, :in_reply_to_tweetid, :in_reply_to_userid])
        # get flagged users
        @debug "$(DTG()) - getting flagged users"
        fu = Set(udf.userid) 
        # find retweets of non-flagged users
        @debug "$(DTG()) - getting retweets"
        retweets = unique(filter(row-> ismissing(row.retweet_userid), dropmissing(mdf, [:retweet_tweetid])).retweet_tweetid)
        # find replies to non-flagged users
        @debug "$(DTG()) - getting replies"
        replies = unique(filter(row-> row.in_reply_to_userid ∉ fu, dropmissing(mdf, [:in_reply_to_tweetid])).in_reply_to_tweetid)
        # write away the results
        @debug "$(DTG()) - writing results"
        open(joinpath(path, "$(fname) retweets.csv"), "w") do io
            for i in retweets
                print(io,i,"\n")
            end
        end

        open(joinpath(path, "$(fname) replies.csv"), "w") do io
            for i in replies
                print(io,i,"\n")
            end
        end
        @debug "$(DTG()) - get_externals finished"
    end

    """
        readjsonl(p::String; kind::Symbol=:retweet)

    Read external tweets from jsonl file into dataframe holding user id, user description & post id

    # Arguments: 
    - `p::String`: the jsonl file contains the additional tweets that are required for the interaction graph
    - `kind:::Symbol`: kind of interacion you want (this is should occur in the filename)
    
    Verify this works as intended: IT DOES NOT: 
    currently reads a folder and we don't want that => make it work for a single file
    """
    function readjsonl(p::String; kind::Symbol=:retweet)
        f = joinpath(p, filter(x-> occursin(isequal(kind, :retweet) ? "retweet" : "reply", x) && occursin("jsonl", x),readdir(p))[1])
        @debug "$(DTG()) - reading $(f)"
        msgids = Vector{Int64}() 
        userids = Vector{String}() # user ids
        userdes = Vector{String}() # user description

        for line in eachline(f)
            res = JSON3.read(line)
            push!(msgids, parse(Int64, res["id_str"]))
            push!(userids, res["user"]["id_str"])
            push!(userdes, res["user"]["screen_name"])
            @debug "$(DTG()) - msg $(res["id_str"]) / $(res["id"]) from user ID $(res["user"]["id_str"]) ($(res["user"]["screen_name"]))" maxlog=1
        end
        return DataFrame([:msgid => msgids, :userid => userids, :user_screen_name => userdes])
    end

    """
    readjsonlnew(p::String)

    Read external tweets from jsonl file into dataframe holding: msgid, userid & user_screen_name

    This external file is the result of "hydrating tweets that were identified in the initial data set of the Twitter Information Operations dataset
    """
    function readjsonlnew(p::String)
        @debug "$(DTG()) - reading $(p)"
        msgids = Vector{Int64}() 
        userids = Vector{String}() # user ids
        userdes = Vector{String}() # user description

        for line in eachline(p)
            res = JSON3.read(line)
            push!(msgids, parse(Int64, res["id_str"])) # Stored as integers
            push!(userids, res["user"]["id_str"])
            push!(userdes, res["user"]["screen_name"])
            @debug "$(DTG()) - msg $(res["id_str"]) / $(res["id"]) from user ID $(res["user"]["id_str"]) ($(res["user"]["screen_name"]))" maxlog=1
        end

        return DataFrame([:msgid => msgids, :userid => userids, :user_screen_name => userdes])
    end

    """
        directedretweetgraph(msgdf::DataFrame,msgmap::Dict, Uinfo::Dict, RUinfo::Dict)

    For a Twitter Misinformation Dataset, build directed & weighted graph from retweet interactions
    
    # Arguments
    - msgdf: DataFrame containing the parsed messages from Twitter Information Operations source
    - msgmap: Dict that maps messageid to a userid (linking external message to a user)
    - Uinfo: Dict that maps nodeid (integer value starting at 1) to a dictionary holding userid, user_screen_name & flagged status
    - RUinfo: Dict that maps userid (string) to nodeid (integer)

    # Method
    FOR all messages DO

        IF original message is coming from a flagged user
            add edge user > flagged user (info flow)
        ELSEIF retweeted message is coming from an external user
            IF the message has been recovered
                add edge external users > flagged user (info flow)
            ELSE 
                skip the message ("weakness to method")

    # Notes
    if graph is established for multiple interaction types, the number of non-interacting nodes is likely to be higher.

    # See also
    ['directedreplygraph'](@ref), ['readjsonlnew'](@ref), ['loaddata'](@ref)
    """
    function directedretweetgraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict)
        # generate the graph, total number of users infered from Uinfo dict
        G = MetaDiGraph(length(Uinfo)) 
        # add nodes + node info to the graph
        for (node, atts) in Uinfo
            set_props!(G, node, atts)
        end
        # get edges from a mapping that holds the edge and its weight
        edges = Dict{Edge, Int}() 
        for row in eachrow(filter(row -> row.is_retweet, msgdf)) # filter row retweets only
            src = nothing
            # determine source node
            if ismissing(row.retweet_userid) # if this is absent, we are dealing with an external tweet
                if haskey(msgmap, row.retweet_tweetid) # if it was recovered, we have a matching user
                    src = RUinfo[msgmap[row.retweet_tweetid]] # return node id from user id
                end
            else
                src = RUinfo[row.retweet_userid]
            end
            if isnothing(src)
                continue
            else
                dst = RUinfo[row.userid]
                e = Edge(src, dst)
                edges[e] = get!(edges, e, 0) + 1
            end
        end
        # add edges to the graph
        for (e,w) in edges
            add_edge!(G, e)
            set_prop!(G,e, :weight, w)
        end

        return G
    end

    """
        directedreplygraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict, flagged_users::Set)

    For a Twitter Misinformation Dataset, build directed & weighted graph from reply interactions
    
    # Arguments
    - msgdf: DataFrame containing the parsed messages from Twitter Information Operations source
    - msgmap: Dict that maps messageid to a userid (linking external message to a user)
    - Uinfo: Dict that maps nodeid (integer value starting at 1) to a dictionary holding userid, user_screen_name & flagged status
    - RUinfo: Dict that maps userid (string) to nodeid (integer)
    - flagged_users: Set of users that are flagged

    # Method
    FOR all messages DO

        IF original message is coming from a flagged user
            add edge user > flagged user (info flow)
        ELSEIF replies message is coming from an external user
            IF the message has been recovered
                add edge external users > flagged user (info flow)
            ELSE 
                skip the message ("weakness to method")

    # Examples
    ```julia
    nothing
    ```

    # Notes
    if graph is established for multiple interaction types, the number of non-interacting nodes is likely to be higher.

    # See also
    ['directedretweetgraph'](@ref), ['readjsonlnew'](@ref), ['loaddata'](@ref)

    """
    function directedreplygraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict, flagged_users::Set)
        # generate the graph
        G = MetaDiGraph(length(Uinfo))
        ## add nodes & node info
        for (node, atts) in Uinfo
            set_props!(G, node, atts)
        end
        # get edges from a mapping that holds the edge and its weight
        edges = Dict{Edge, Int}()
        # filter replt rows only
        for row in eachrow(dropmissing(msgdf, [:in_reply_to_tweetid] )) 
            src = nothing
            if row.in_reply_to_userid ∈ flagged_users # if user is flagged: get its id directly
                src = RUinfo[row.in_reply_to_userid]
            else # if it is an external user and recovered the twee: get id
                if haskey(msgmap, row.in_reply_to_tweetid)
                    src = RUinfo[msgmap[row.in_reply_to_tweetid]]
                end
            end
            if isnothing(src)
                continue
            else
                dst = RUinfo[row.userid]
                e = Edge(src, dst)
                edges[e] = get!(edges, e, 0) + 1
            end
        end
        # add edges to graph
        for (e,w) in edges
            add_edge!(G, e)
            set_prop!(G,e, :weight, w)
        end

        return G
    end

    """
        directedbipartiteretweetgraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict,)

    Generate directed bipartite retweet graph. Layer 1 = users, layer 2 = posts

    # Arguments:
    - `msgdf::DataFrame`: dataframe containing the message data in the dataset
    - `msgmap::Dict`: mapping of post id to user id
    - `Uinfo::Dict`: mapping of node if on graph to dict (twitter user id, user description, flagged status)
    - `RUinfo::Dict`: reverse mapping of Uinfo; maps twitter user id to node id
    
    """
    function directedbipartiteretweetgraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict)
        # generate the graph
        G = MetaDiGraph(length(Uinfo))
        ## add nodess & info
        # user nodes
        Nu = length(Uinfo)
        for (node, atts) in Uinfo
            set_props!(G, node, atts)
        end
        # post info is a mapping from post id to node id
        Pinfo = Dict{}()
        # posts from flagged users:
        p_1 = unique(filter(row -> !ismissing(row.retweet_userid) , msgdf).retweet_tweetid)
        # posts from non-flagged users that are recovered by hydration
        p_2 = unique(filter(row ->  ismissing(row.retweet_userid) && 
                                    !ismissing(row.retweet_tweetid) && 
                                    haskey(msgmap, row.retweet_tweetid), 
                            msgdf).retweet_tweetid)
        for post in union(p_1, p_2)
            # add new node 
            add_vertex!(G)
            @debug "number of vertices: $(nv(G))" maxlog=10
            # set settings
            set_props!(G, nv(G), Dict(:userid => post, :Label => "post", :flagged => false))
            Pinfo[post] = nv(G)
        end
        # reverse post mapping leading to:  node_id > post_id
        RPinfo = Dict(val => key for (key, val) in Pinfo)
        Np = length(Pinfo)


        ## add edges
        for row in eachrow(filter(row -> row.is_retweet, msgdf)) # filter row retweets only
            # edge from author to msg
            if ismissing(row.retweet_userid)
                if haskey(msgmap, row.retweet_tweetid) # in case of recovered tweet
                    # add edge from author to message (author_of_tweet_id > tweet_id) 
                    aut = RUinfo[msgmap[row.retweet_tweetid]]
                    msg = Pinfo[row.retweet_tweetid]
                    add_edge!(G, aut, msg)
                    @debug "adding authorship from user $(msgmap[row.retweet_tweetid]) ($(src)) to msg $(row.retweet_tweetid) ($(dst))" maxlog=1
                    # add edge from message to spreader (tweet_id > userid)
                    add_edge!(G, msg, RUinfo[row.userid])
                end
            else
                # add edge from author to message (retweet_userid > tweet_id)
                aut = RUinfo[row.retweet_userid]
                msg = Pinfo[row.retweet_tweetid]
                add_edge!(G, aut, msg)
                # add edge from message to spreader
                add_edge!(G, msg, RUinfo[row.userid])
            end
        end

        set_props!(G, Dict(:Nu => Nu, :Np => Np, :kind => :retweet))

        return Pinfo, RPinfo, G
    end

    """
        directedbipartitereplygraph(msgmap::Dict, Uinfo::Dict, RUinfo::Dict, msgdf::DataFrame)

    Generate directed bipartite reply graph. Layer 1 = users, layer 2 = posts

    # Arguments:
    - `msgmap::Dict`: mapping of post id to user id
    - `Uinfo::Dict`: mapping of node if on graph to dict (twitter user id, user description, flagged status)
    - `RUinfo::Dict`: reverse mapping of Uinfo; maps twitter user id to node id
    - `msgdf::DataFrame`: dataframe containing the message data in the dataset
    - `flagged_users::Set`: set of the flagged users
    """
    function directedbipartitereplygraph(msgdf::DataFrame, msgmap::Dict, Uinfo::Dict, RUinfo::Dict, flagged_users::Set)
        # filter replies only
        mdf = dropmissing(msgdf, :in_reply_to_tweetid)
        G = MetaDiGraph(length(Uinfo))
        ## add nodes + info
        # user nodes
        Nu = length(Uinfo)
        for (node, atts) in Uinfo
            set_props!(G, node, atts)
        end
        # message mapping (post id => node id)
        Pinfo = Dict{}()
        # posts from flagged users
        p_1 = unique(filter(row -> row.in_reply_to_userid ∈ flagged_users, mdf).in_reply_to_tweetid)
        # posts from non-flagged users that are recovered by hydration
        p_2 = unique(filter(row -> row.in_reply_to_userid ∉ flagged_users && haskey(msgmap, row.in_reply_to_tweetid), mdf).in_reply_to_tweetid)
        # add post vertices
        for post in union(p_1, p_2)
                add_vertex!(G)
                @debug "number of vertices: $(nv(G))" maxlog=10
                set_props!(G, nv(G), Dict(:userid => post, :Label => "post", :flagged => false))
                Pinfo[post] = nv(G)
        end
        RPinfo = Dict(val => key for (key, val) in Pinfo)
        Np = length(Pinfo)

        ## add edges
        for row in eachrow(mdf)
            if row.in_reply_to_userid ∈ flagged_users
                # case of original message by flagged user
                aut = RUinfo[row.in_reply_to_userid]
                msg = Pinfo[row.in_reply_to_tweetid]
                add_edge!(G, aut, msg)
                @debug "adding flagged authorship from user $(aut) to msg $(msg)" maxlog=1
                # add edge from message to spreader
                add_edge!(G, msg, RUinfo[row.userid])
            else
                if haskey(msgmap, row.in_reply_to_tweetid)
                    # case of reply to recovered message from external user
                    aut = RUinfo[msgmap[row.in_reply_to_tweetid]]
                    msg = Pinfo[row.in_reply_to_tweetid]
                    add_edge!(G, aut, msg)
                    @debug "adding authorship from user $(aut) to msg $(msg)" maxlog=1
                    # add edge from message to spreader
                    add_edge!(G, msg, RUinfo[row.userid])
                end
            end
        end

        set_props!(G, Dict(:Nu => Nu, :Np => Np, :kind => :reply))

        return Pinfo, RPinfo, G
    end

    """
        grapher(path::String)

    Generate interaction graphs from a Twitter misinformation data file. Considers both retweets and replies. In the interaction graphs, 
    users from both interactions will be included in order to allow multi-layer community detection based on the Leiden method.

    """
    function grapher(msgfile::Union{String,Array{String,1}}, userfile::Union{String,Array{String,1}}; α::Float64=0.05)
        #@assert occursin("tweets", msgfile)
        #@assert occursin("user", userfile)
        @debug "$(DTG()) - building graphs for $(msgfile)"
        # get raw data as dataframe
        usrdf, msgdf = loaddata(userfile, msgfile) # check this is working as intended
        
        # get external messages as dataframe
        @debug "$(DTG()) - loading external retweets"
        rtdf = isa(msgfile, String) ? readjsonlnew(msgfile*"_retweets.jsonl") : vcat([readjsonlnew(file*"_retweets.jsonl") for file in msgfile]...)
        @debug "$(DTG()) - loading external replies"
        rpdf = isa(msgfile, String) ? readjsonlnew(msgfile*"_replies.jsonl") : vcat([readjsonlnew(file*"_replies.jsonl") for file in msgfile]...)

        # establish nodes in graph
        Uinfo = Dict{Int64, Dict}()
        # add flagged users
        for row in eachrow(unique(usrdf, :userid))
            Uinfo[length(Uinfo) + 1] = Dict(:userid => row.userid, :Label => row.user_screen_name, :flagged => true)
        end
        # get other users from retweets
        for row in eachrow(unique(rtdf, :userid))
            Uinfo[length(Uinfo) + 1] = Dict(:userid => row.userid, :Label => row.user_screen_name, :flagged => false)
        end
        # reverse user mapping
        RUinfo = Dict(val[:userid] => key for (key, val) in Uinfo)

        # add other users from replies
        @debug "starting with replies"
        for row in eachrow(unique(rpdf, :userid))
            haskey(RUinfo, row.userid) && continue # check if user already known
            # if not, add them
            @debug "adding $(row.userid) to graph with node number $(length(Uinfo) + 1)" maxlog=10
            Uinfo[length(Uinfo) + 1] = Dict(:userid => row.userid, :Label => row.user_screen_name, :flagged => false)
            RUinfo[row.userid] = length(Uinfo)
        end

        # map messages id > user id
        ## map additional messages to user
        uniquemsg_rt = unique(rtdf, :msgid)
        msgmap_rt = Dict(zip(uniquemsg_rt.msgid, uniquemsg_rt.userid))
        uniquemsg_rp = unique(rpdf, :msgid)
        msgmap_rp = Dict(zip(uniquemsg_rp.msgid, uniquemsg_rp.userid))

        # generate graphs
        G_rt = directedretweetgraph(msgdf, msgmap_rt, Uinfo, RUinfo)
        G_rp = directedreplygraph(msgdf, msgmap_rp, Uinfo, RUinfo, Set(usrdf.userid))
        G_rt_bp = directedbipartiteretweetgraph(msgdf, msgmap_rt, Uinfo, RUinfo)[3]
        G_rp_bp = directedbipartitereplygraph(msgdf, msgmap_rp, Uinfo, RUinfo, Set(usrdf.userid))[3]
        G_rt_bp_proj = projectbipartite(G_rt_bp, α=α)[4] 
        G_rp_bp_proj = projectbipartite(G_rp_bp, α=α)[4] 
        
        @debug "retweet edge reduction $(round(ne(G_rt_bp_proj) / ne(G_rt) * 100))%"
        @debug "reply edge reduction $(round(ne(G_rp_bp_proj) / ne(G_rp) * 100))%"

        #return round(ne(G_rt_bp_proj) / ne(G_rt) * 100, digits=2), round(ne(G_rp_bp_proj) / ne(G_rp) * 100, digits=2), G_rt_bp_proj
        return G_rt, G_rp, G_rt_bp, G_rp_bp, G_rt_bp_proj, G_rp_bp_proj, Uinfo, RUinfo

    end


    """
    writeout to a edge and node file that can be read by gephi
    """
    function graphwriter(G::Union{MetaGraph, MetaDiGraph})
        io = IOBuffer()
        ## PART VERTICES
        println(io,"Id; Label; flagged")
        for v in vertices(G)
            println(io,"$(v); $(props(G, v)[:Label]); $(props(G, v)[:flagged])")
        end
        # write to file        
        open("nodelist.csv", "w") do f
            print(f, String(take!(io)))
        end

        ## PART EDGES
        println(io,"Source; Target; Weight")
        for e in edges(G)
            println(io, "$(e.src); $(e.dst); $(props(G, e.src, e.dst)[:weight])")
        end
        open("edgelist.csv", "w") do f
            print(f, String(take!(io)))
        end

    end
    """
        graphbuilder(path::String; kind::Symbol=:retweet, α::Float64=0.001)
    
    Main function for graph generation. Reads path, loads up the data & builds MetaGraphs
    
    # Arguments
    - `path::String`: folder holding the data
    - `kind`::Symbol: type of interaction graph you want to build
    - `α::Float64`: limit for statistical significance of V-motif

    returns:
        - directed weighted interaction graph
        - directed bipartite interaction graph
        - projected binary interaction graph
        - observed V-motifs
        - expected V-motifs
        - p-values for projected user-interactions
    """
    function graphbuilder(path::String; kind::Symbol=:retweet, α::Float64=1e-2)
        @debug "$(DTG()) - building $(kind) graph"
        ## load up data from desinfo dataset
        usrdf, msgdf = loaddata(path,   usrfields=[:userid, :user_screen_name],
                                        msgfields=[:tweetid, :userid, :is_retweet, :retweet_userid, :retweet_tweetid, :in_reply_to_tweetid, :in_reply_to_userid])

        ## original messages per kind (as downloaded by hydrator) as DataFrame 
        rtsdf = readjsonl(path, kind=kind)
        
        ## extract the user information
        Uinfo = Dict{Int64, Dict}()
        # get flagged users
        for row in eachrow(unique(usrdf, :userid))
            Uinfo[length(Uinfo) + 1] = Dict(:userid => row.userid, :Label => row.user_screen_name, :flagged => true)
        end
        # get other users
        for row in eachrow(unique(rtsdf, :userid))
            Uinfo[length(Uinfo) + 1] = Dict(:userid => row.userid, :Label => row.user_screen_name, :flagged => false)
        end
        # reverse user information
        RUinfo = Dict(val[:userid] => key for (key, val) in Uinfo)

         
        ## map additional messages to user
        uniquemsg = unique(rtsdf, :msgid)
        msgmap = Dict(zip(uniquemsg.msgid, uniquemsg.userid)) ##  maybe use integer value from the start???

        ## generate directed interaction network (retweet/reply)
        Gint = isequal(kind, :retweet) ? directedretweetgraph(msgmap, Uinfo, RUinfo, msgdf) : directedreplygraph(msgmap, Uinfo, RUinfo, msgdf, Set(usrdf.userid))
        
        ## generate directed bipartite usr-msg network (retweet/reply)
        Gbip = isequal(kind, :retweet) ? directedbipartiteretweetgraph(msgmap, Uinfo, RUinfo, msgdf)[3] :  directedbipartitereplygraph(msgmap, Uinfo, RUinfo, msgdf, Set(usrdf.userid))[3]

        ## generate projected user network
        V_star, V_exp, p_vals, Gproj = projectbipartite(Gbip, α=α)

        return Gint, Gbip, Gproj, V_star, V_exp, p_vals
        
    end

    "helper function to calculate the p-value of the poisson-binomial approximation"
    pval(v_expected, v_observed) = 1 - cdf(Poisson(v_expected), v_observed)

    "function to map bipartite user-post to user-user"
    function projectbipartite(G::MetaDiGraph; α::Float64=0.001)
        @debug "$(DTG()) - projecting graph with α = $(α)"
        # degrees of all users:
        k_λ_in =  indegree(G, 1 : get_prop(G, :Nu))
        k_λ_out = outdegree(G, 1 : get_prop(G, :Nu))
        # adjacency matrix:
        A = LightGraphs.LinAlg.adjacency_matrix(G)
        # real V motif matrix (sparse representation)
        Nu = get_prop(G, :Nu)
        V_star = A[1:Nu,:]*A[:,1:Nu]
        # expected V-motif matrix (sparse representation)
        # get locations and values
        I, J, V_star_val = findnz(V_star)
        V_exp_val = k_λ_out[I] .* k_λ_in[J] / get_prop(G, :Np)
        #expected_v_motifs(V_star, k_λ_out, k_λ_in, get_prop(G, :Np))#k_λ_out * k_λ_in' /get_prop(G, :Np)
        # p-values (sparse)
        p_vals = pval.(V_exp_val, V_star_val)
        #spzeros(Float64,get_prop(G, :Nu),get_prop(G, :Nu))#pval.(V_exp, V_star)
        # FDR procedure Benjamini–Hochberg
        #I, J, V = findnz(p_vals)
        k = sortperm(p_vals) # costly (1e6 edges: 0.09s; 1e7 edges: 1.57s; 1e8 elements: 23s)
        m = length(k)
        #@warn V_star[3121, 348], V_exp[3121, 348], p_vals[3121, 348]
        pvalfilter = p_vals .<= k/m*α # probleem!
        # generate graph from significant sparse edges
        A_filt = sparse(I[pvalfilter], J[pvalfilter], ones(Int,sum(pvalfilter)), Nu, Nu)
        G_filt = MetaDiGraph(A_filt)
        for i = 1:Nu
            set_props!(G_filt, i, props(G, i))
        end
        set_prop!(G_filt, :Nu, Nu)

        return V_star, sparse(I,J,V_exp_val), sparse(I,J,p_vals), G_filt
    end

    """
        Coherence_checks(G:MetaDiGraph)

    check if bipartite graph is built up as it should
    """
    function coherence_checks(GBip::MetaDiGraph) 
        # degrees of all posts:
        k_γ_in =  indegree(GBip, get_prop(GBip, :Nu) + 1 : get_prop(GBip,:Nu) + get_prop(GBip, :Np))
        k_γ_out = outdegree(GBip, get_prop(GBip, :Nu) + 1 : get_prop(GBip,:Nu) + get_prop(GBip, :Np))
        # degrees of all users:
        k_λ_in =  indegree(GBip, 1 : get_prop(GBip, :Nu))
        k_λ_out = outdegree(GBip, 1 : get_prop(GBip, :Nu))
        @info """Running coherence checks for the graph\n
            $(get_prop(GBip, :Nu)) user nodes, $(get_prop(GBip, :Np)) msg nodes
            
            - indegree of posts equal to 1: $(all(isequal.(k_γ_in, 1)) ? "OK" : "PROBLEM!")
            - sum of in/out degrees of users and posts equal to one another: $(isequal(sum(map(sum, [k_γ_in, k_γ_out])), sum(map(sum, [k_λ_in, k_λ_out]))) ? "OK" : "PROBLEM")

            """
    end

    """
    degreeplotter(G::MetaDiGraph; fname::String=pwd())

    Generate illustration of in- and outdegrees and their distributions.
    illustrations are stored in the `img` subfolder of `path`
    """
    function degreeplotter(G::MetaDiGraph; path::String=pwd())
        # outpath and settings
        isdir(joinpath(path,"img")) ? mkdir(joinpath(path,"img")) : nothing
        outpath = joinpath(path,"img")
        fname = filter!(x->length(x)>0,split(dpath,"/"))[end]

        ## all nodes, showing colors for tagged data
        # indegree
        @debug "plotting indegree"
        Nu = nv(G)
        y = indegree(G,1:Nu)
        c = map(x-> get_prop(G, x ,:flagged) == true ? :red : :green, 1:Nu)
        pin =  scatter(1:Nu ,y, color=c, marker=:circle, label="", size=(2000, 1200))
        title!("projected graph indegree")
        xlabel!("node number")
        ylabel!("indegree")
        savefig(pin, joinpath(outpath,"$(fname) indegree.png"))
        # outdegree

        @debug "plotting outdegree"
        y = outdegree(G,1:Nu)
        c = map(x-> get_prop(G, x ,:flagged) == true ? :red : :green, 1:Nu)
        pout =  scatter(1:Nu, y, color=c, marker=:circle, label="", size=(2000, 1200))
        title!("projected graph outdegree")
        xlabel!("node number")
        ylabel!("outdegree")
        savefig(pout, joinpath(outpath,"$(fname) outdegree.png"))

        ## Degree distributions (global and per type)
        function minimapper(v)
            res = countmap(v)
            x = [p.first for p in res]
            f = [p.second for p in res] / length(v)
            return x,f
        end

        flaggedfilter = map(x-> get_prop(G, x ,:flagged) , 1:get_prop(G,:Nu))
        
        # indegree
        @debug "plotting indegree distribution"
        ppin = scatter(minimapper(indegree(G))..., label="global", yscale=:log10)
        scatter!(minimapper(indegree(G, findall(flaggedfilter)))..., label="flagged")
        scatter!(minimapper(indegree(G, findall(x-> iszero(x),  flaggedfilter)))..., label="not-flagged")
        title!("indegree distribution")
        xlabel!("indegree")
        ylabel!("frequency")
        savefig(ppin, joinpath(outpath,"$(fname) indegree distribution.png"))

        # outdegree
        @debug "plotting outdegree distribution"
        ppout = scatter(minimapper(outdegree(G)), label="global", yscale=:log10)
        scatter!(minimapper(outdegree(G, findall(flaggedfilter)))..., label="flagged")
        scatter!(minimapper(outdegree(G, findall(x-> iszero(x),  flaggedfilter)))..., label="not-flagged")
        title!("outdegree distribution")
        xlabel!("outdegree")
        ylabel!("frequency")
        savefig(ppout, joinpath(outpath,"img","$(fname) outdegree distribution.png"))
        
    end


    """
    function that does everything (including LaTeX table)
    """
    function global_loader(globalpath::String)
        @info "starting up"
        @assert isdir(globalpath)
        # buffer to write to
        io = IOBuffer()
        println(io, """\\begin{table*}[]\n\\begin{centering}\n\\begin{array}{lrlrlllr}\n\\hline""")
        println(io, "\\text{Dataset} & N_{\\text{retweets}} & \\%_{\\text{recovered}} & N_{\\text{replies}} & \\%_{\\text{recovered}} & \\%_{\\text{matched edges, retweet}} & \\%_{\\text{matched edges, reply}} \\\\ \\hline")
        # read folders
        for subfolder in filter(x-> isdir(joinpath(globalpath,x)), readdir(globalpath))
            # get name/ref
            @info "working on $subfolder"
            # get relevant files
            msgfiles = filter(x->x[end-3:end] == ".csv" && occursin("tweets_csv_hashed",x) , readdir(joinpath(globalpath, subfolder)))
            usrfiles = filter(x->x[end-3:end] == ".csv" && occursin("users_csv_hashed",x) , readdir(joinpath(globalpath, subfolder)))
            @info "\n\tmsgfiles: $(msgfiles)\n\tuserfiles: $(usrfiles)"

            # read log information
            tot_rt = 0; rec_rt = 0;
            tot_rp = 0; rec_rp = 0;
            for msg in msgfiles
                # read the log
                s = readchomp(joinpath(globalpath, subfolder, "$(msg)_retweets.log")) # for retweets
                c = parse.(Int,match(r"Total: (\d+), collected (\d+)", s).captures)
                tot_rt += c[1]
                rec_rt += c[2]
                
                s = readchomp(joinpath(globalpath, subfolder, "$(msg)_replies.log")) # for replies
                c = parse.(Int,match(r"Total: (\d+), collected (\d+)", s).captures)
                tot_rp += c[1]
                rec_rp += c[2]
                # extract value
            end
            
            # build the graphs
            pct_rtedges = 0.
            pct_rpedges = 0.
            gproj = nothing
            try
                pct_rtedges, pct_rpedges, gproj = grapher(joinpath.(globalpath, subfolder ,msgfiles), 
                                               joinpath.(globalpath, subfolder, usrfiles), α=0.05)
            catch
                @warn "problem with $(subfolder)"
            end
            @info "$(subfolder), $(round(rec_rt / tot_rt * 100, digits=2)), $(round(rec_rp / tot_rp * 100, digits=2)) "
            
            println(io, "\\text{$(subfolder)} & $(tot_rt) & $(round(rec_rt / tot_rt * 100, digits=2)) & $(tot_rp) & $(round(rec_rp / tot_rp * 100, digits=2)) & $(pct_rtedges) & $(pct_rpedges) & $(!isnothing(gproj) ? ne(gproj) : 0.0) \\\\")


        end

        println(io, "\\end{array}\n\\par\\end{centering}\n\\end{table*}")
        # write result to file
        open(joinpath(globalpath,"overview.log"), "w") do f
            write(f, String(take!(io)))
        end
        
    end

end
