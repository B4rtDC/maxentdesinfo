### A Pluto.jl notebook ###
# v0.16.1

using Markdown
using InteractiveUtils

# ╔═╡ c41fb568-25bd-11ec-1f56-d1fbf2dd4e33
# Load up modules for working
begin
	using Pkg
	cd(dirname(@__FILE__))
	Pkg.activate(".")
	@info pwd()
	
	function ingredients(path::String)
		name = Symbol(basename(path))
		m = Module(name)
		Core.eval(m,
			Expr(:toplevel,
				 :(eval(x) = $(Expr(:core, :eval))($name, x)),
				 :(include(x) = $(Expr(:top, :include))($name, x)),
				 :(include(mapexpr::Function, x) = $(Expr(:top, :include))(mapexpr, $name, x)),
				 :(include($path))))
		m
	end
	
	Analyse = ingredients("./src/Analyse.jl").Analyse
end

# ╔═╡ 770505bc-19cb-44c4-b8de-30e37234bc68
md"""
Notebook showing the entire pipeline for Twitter information operations report datasets

"""

# ╔═╡ d2c74016-f98a-4902-8c61-ba0a33195d47
md"""
we enlarge the default notebook width
"""

# ╔═╡ 7a60c92e-9d74-492b-970a-b271896c85d4
# Make cells wider
html"""<style>
/*              screen size more than:                     and  less than:                     */
@media screen and (max-width: 699px) { /* Tablet */ 
  /* Nest everything into here */
    main { /* Same as before */
        max-width: 1200px !important; /* Same as before */
        margin-right: 200px !important; /* Same as before */
    } /* Same as before*/

}

@media screen and (min-width: 700px) and (max-width: 1199px) { /* Laptop*/ 
  /* Nest everything into here */
    main { /* Same as before */
        max-width: 1200px !important; /* Same as before */
        margin-right: 200px !important; /* Same as before */
    } /* Same as before*/
}

@media screen and (min-width:1200px) and (max-width: 1920px) { /* Desktop */ 
  /* Nest everything into here */
    main { /* Same as before */
        max-width: 1200px !important; /* Same as before */
        margin-right: 200px !important; /* Same as before */
    } /* Same as before*/
}

@media screen and (min-width:1921px) { /* Stadium */ 
  /* Nest everything into here */
    main { /* Same as before */
        max-width: 1200px !important; /* Same as before */
        margin-right: 200px !important; /* Same as before */
    } /* Same as before*/
}
</style>
"""

# ╔═╡ 67adb4ee-a0dc-47ca-bd0d-67d3b2687ee4
md"""
We load up the analysis modules
"""

# ╔═╡ eebae507-271f-4935-8f13-ca25bd1907f6
datapath = "./bigdata"

# ╔═╡ b0525f84-427f-400c-8f42-189b84283d42
readdir("./bigdata/")

# ╔═╡ 07fa28e4-ea9f-4bd9-a0ad-38f3f8d94a47
md"""
## Get external tweet ids
the function below will identify the external tweets to recover and write them out to as csv file.
"""

# ╔═╡ f7549366-d538-4900-b4e2-85d28169925c
Analyse.get_externals(datapath)

# ╔═╡ 3036b5bd-77c5-441a-bc38-bf23bb56b12c
md"""## Download external tweet ids
cf. `./src/hydrate.py` & `./src/hydratorpipeline.py`"""

# ╔═╡ 3be71733-6e2f-4ec3-83ec-e46bcc872371
md"""
## Generate interaction networks

"""

# ╔═╡ 0ee608f7-5c64-4051-bc19-7f64e04305a5
begin
	msgfiles = "./bigdata/honduras_022020_tweets_csv_hashed.csv"
	usrfiles = "./bigdata/honduras_022020_users_csv_hashed.csv"
	G_rt, G_rp, G_rt_bp, G_rp_bp, G_rt_bp_proj, G_rp_bp_proj, Uinfo, RUinfo = Analyse.grapher(msgfiles, usrfiles)
end

# ╔═╡ 400c36ff-d96a-482a-8d92-48e5d6407bd1


# ╔═╡ Cell order:
# ╟─770505bc-19cb-44c4-b8de-30e37234bc68
# ╟─d2c74016-f98a-4902-8c61-ba0a33195d47
# ╟─7a60c92e-9d74-492b-970a-b271896c85d4
# ╟─67adb4ee-a0dc-47ca-bd0d-67d3b2687ee4
# ╠═c41fb568-25bd-11ec-1f56-d1fbf2dd4e33
# ╠═eebae507-271f-4935-8f13-ca25bd1907f6
# ╠═b0525f84-427f-400c-8f42-189b84283d42
# ╟─07fa28e4-ea9f-4bd9-a0ad-38f3f8d94a47
# ╠═f7549366-d538-4900-b4e2-85d28169925c
# ╟─3036b5bd-77c5-441a-bc38-bf23bb56b12c
# ╟─3be71733-6e2f-4ec3-83ec-e46bcc872371
# ╠═0ee608f7-5c64-4051-bc19-7f64e04305a5
# ╠═400c36ff-d96a-482a-8d92-48e5d6407bd1
