# -*- coding: utf-8 -*-
"""Generate booking-format fixtures (one file per parser branch) into tests/fixtures/.
Each fixture is crafted to TRIGGER its intended parser branch. The snapshot harness
records whatever the current code does with each — that's the golden baseline.
Run once: python tests/_make_fixtures.py
"""
import io, os
D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
os.makedirs(D, exist_ok=True)
TAB = "\t"

F = {}  # name -> content

# 1. Bare Cinemark DMA/SALES/#/THEATRE/SCR (one-per-line; blank action). Allie/Kellen.
F["01_bare_cinemark_dma_sales.txt"] = "\n".join(
    ["DMA","SALES","#","THEATRE","SCR"," "] +
    sum(([d,"Allie Fullmer",u,t,s,(a if a else " ")] for d,u,t,s,a in [
        ("Houston, TX","216","Tinseltown 17 + XD (The Woodlands, TX)","17","Final"),
        ("Houston, TX","1079","Cinemark Spring-Klein + XD (Spring, TX)","12",""),
        ("Houston, TX","295","Cinemark 18 + XD (Webster, TX)","18","Final"),
        ("Austin, TX","384","Southpark Meadows 14 (Austin, TX)","14","Final"),
    ]), []))

# 2. ComScore "Theater #" Gundrum (Theater#/Name/Screens/City/DMA/Film(date), one-per-line)
F["02_comscore_theater_hash_gundrum.txt"] = "\n".join(
    ["Theater #","Theater Name","Theater Screens","Theater City","DMA","Animal Farm","(5/1/2026)"] +
    sum(([u,n,s,c,dma,(a if a else " ")] for u,n,s,c,dma,a in [
        ("202","Tinseltown USA","17","Rochester","Rochester, NY","Final"),
        ("37","Movies 10","10","Wilmington","Philadelphia, DE","Final"),
        ("1151","Cinemark Watchung and XD","10","Watchung","New York, NY, NJ",""),
        ("243","Tinseltown","14","Salisbury","Charlotte, NC","Final"),
    ]), []))

# 3. Mary Ann headerless 3-col (Theatre  Film  Action) tab-separated
F["03_mary_ann_3col.txt"] = "\n".join(TAB.join(r) for r in [
    ("Aberdeen","Animal Farm","Final"),
    ("Bloomington","Animal Farm","Hold"),
    ("Minot","Animal Farm","Open"),
    ("Bluefield","Animal Farm","Final"),
])

# 4. snake_case Cinemark DB export
F["04_snake_case.txt"] = "\n".join([
    "dma_name","theater_name","city","state","title","status",
    "Houston","Cinemark Webster 18","Webster","TX","Animal Farm","Final",
    "Austin","Cinemark Round Rock 14","Round Rock","TX","Animal Farm","Hold",
])

# 5. dunder __COLUMN__
F["05_dunder_column.txt"] = "\n".join([
    "__DMA__","__SALES__","__#__","__THEATRE__","__SCR__","____",
    "Houston","Allie","216","Cinemark Webster 18","18","Final",
    "Houston","Allie","295","Cinemark Katy 19","19","",
])

# 6. DMA/City/Theatre/Title 8-col (Eric Bond)
F["06_dma_city_theatre_title.txt"] = "\n".join([
    "DMA","City","Theatre","Title","Print","Attributes","Status","Detail",
    "Dallas - Ft. Worth","Plano","Cinemark West Plano","Animal Farm","D-Cin","2D","Final","x",
    "Dallas - Ft. Worth","Allen","Cinemark Allen 16","Animal Farm","D-Cin","2D","Hold","y",
])

# 7. ComScore Theatre#/ComScore Name/City/ST/Screens/DMA/Film (Jennifer Hernandez)
F["07_comscore_theatre_city_st.txt"] = "\n".join([
    "Theatre #","ComScore Name, City, State","City","ST","Screens","DMA","Animal Farm",
    "9023","Cinemark 18 (Webster, TX)","Webster","TX","18","Houston","Final",
    "8560","Cinemark 19 (Katy, TX)","Katy","TX","19","Houston","",
])

# 8. Standard Action/Policy
F["08_standard_action.txt"] = "\n".join([
    "Theatre","Film","Action",
    "Cinemark Webster 18","Animal Farm","Final",
    "Cinemark Katy 19","Animal Farm","Open",
])

# 9. Landmark "Location"
F["09_landmark_location.txt"] = "\n".join([
    "Animal Farm","Location",
    "Landmark Guildford 12","finished",
    "Landmark Kitchener 8","holding",
])

# 10. Small-exhibitor "City, ST   FINAL"
F["10_small_exhibitor_citystate.txt"] = "\n".join([
    "Ark City, KS       HOLD",
    "Florence, SC       Final",
    "Guymon, OK         Final",
])

# 11. Gundrum ID# Grid (tab): ID #/Screens/Theatre (City, ST)/DMA/film
F["11_gundrum_id_grid.txt"] = "\n".join([
    "Animal Farm",
    TAB.join(["ID #","Screens","Theatre","DMA","Animal Farm"]),
    TAB.join(["8320","17","Cinemark Tinseltown Fayetteville 17 (Fayetteville, AR)","Ft Smith","Final"]),
    TAB.join(["7568","16","Cinemark Tinseltown 290 Houston (Houston, TX)","Houston","Hold"]),
])

# 12. Diane Johnson circuit grid (tab): CIRCUIT/THEATRE/CITY/STATE/Film - M/D
F["12_diane_johnson.txt"] = "\n".join([
    TAB.join(["CIRCUIT","THEATRE","CITY","STATE","Animal Farm - 5/1"]),
    TAB.join(["Marcus","Marcus Majestic Cinema","Brookfield","WI","Final"]),
    TAB.join(["Marcus","Marcus Point Cinema","Madison","WI","Hold"]),
])

# 13. Glen Parham / GTC (tab): Circuit/Theatre Name/City/ST/Title/DIST/Playwk/Status/WK#/FSS
F["13_glen_parham.txt"] = "\n".join([
    TAB.join(["Circuit","Theatre Name","City","ST","Title","DIST","Playwk","Status","WK#","FSS"]),
    TAB.join(["GTC","GTC Beechwood 18","Athens","GA","Animal Farm","ANG","5/1","Final","2","x"]),
    TAB.join(["GTC","GTC Mall of GA 14","Buford","GA","Animal Farm","ANG","5/1","Hold","2","y"]),
])

# 14. Andy Anderson THEATRE/SCR/City/State (tab)
F["14_andy_anderson.txt"] = "\n".join([
    "Animal Farm",
    TAB.join(["THEATRE","SCR","City","State"]),
    TAB.join(["Century 16 + IMAX","16","San Francisco","CA"]),
    TAB.join(["Century 20 Daly City","20","Daly City","CA"]),
])

# 15. Jennifer Solorzano THEATRE/SCR (tab, blank-header film col)
F["15_jennifer_solorzano.txt"] = "\n".join([
    "Animal Farm",
    TAB.join(["THEATRE","SCR",""]),
    TAB.join(["Cinemark 16 Albuquerque","16","Final"]),
    TAB.join(["Century Rio 24","24","Hold"]),
])

# 16. AMC Film Programmer
F["16_amc.txt"] = "\n".join([
    "AMC Film Programmer",
    "Animal Farm",
    "CHICAGO  AMC River East 21  151 Final - 02/26/2026",
    "CHICAGO  AMC Yorktown 18  565 Holdover",
])

# 17. Kaufman / Malco 5-line blocks
F["17_kaufman.txt"] = "\n".join([
    "FT SMITH ARK","Malco Cinema 12","ANGEL","Animal Farm","F",
    "SOUTHAVEN MS","Malco Stadium 12","ANGEL","Animal Farm","H",
    "JONESBORO AR","Malco Towne 16","ANGEL","Animal Farm","F TU",
])

# 18. Holdover grid (David Saunders) PRELIMINARY HOLD OVERS
F["18_holdover_grid.txt"] = "\n".join([
    "PRELIMINARY HOLD OVERS",
    TAB.join(["THEATRE","FILM","HOLD","Fr","Sa","Su","Mo","Tu","We","Th","UNDECIDED"]),
    TAB.join(["Bainbridge Cinemas","Animal Farm","x","","","","","","","",""]),
    TAB.join(["Pickford Film Center","Animal Farm","","x","x","","","","","",""]),
])

# 19. Cinepolis 3-line-per-venue
F["19_cinepolis.txt"] = "\n".join([
    "Cinepolis Vista (Vista, CA)","FINAL","Animal Farm",
    "Moviehouse & Eatery (Austin, TX)","HOLD","Animal Farm",
])

# 20. Cineplex policy: "2111 - CPX Name" / Title / Screening
F["20_cineplex_policy.txt"] = "\n".join([
    TAB.join(["2111 - CPX McGillivray","Animal Farm","Alternating"]),
    TAB.join(["2222 - CPX Scotiabank","Animal Farm","Multiple Matinees"]),
])

# 21. Watson / Imagine / Cinestarz Canada
F["21_watson.txt"] = "\n".join([
    TAB.join(["Booking Week","Group","Theatre Name","Title","Language","Showtime","O/H","Dist"]),
    TAB.join(["5/1","Imagine","Imagine Market Mall","Animal Farm","EN","mats","H","Angel"]),
    TAB.join(["5/1","Cinestarz","Cinestarz Brampton","Animal Farm","EN","prime","O","Angel"]),
])

# 22. Landmark Canada (Nathan Gendron)
F["22_landmark_canada.txt"] = "\n".join([
    TAB.join(["Studio","Cinema","Film","Status","AM","Early Mat","Late Mat","Early Eve","Late Eve"]),
    TAB.join(["Angel","Landmark Cinemas Guildford","Animal Farm","Hold","0","1","1","1","1"]),
    TAB.join(["Angel","Landmark Cinemas Kanata","Animal Farm","Final","0","0","0","1","1"]),
])

# 23. Blue Smiley / CFB Rentrak grid
F["23_blue_smiley.txt"] = "\n".join([
    TAB.join(["Rentrak ID","CIRCUIT","THEATRE","CITY","ST","TITLE","Studio","Playwk","PLAYDATE","COMMENTS"]),
    TAB.join(["9999","GTC","GTC Auburn 10","Auburn","AL","Animal Farm","Angel","5/1","5/1/2026",""]),
])

# 24. David bring-back: Theater|City,ST|Title|Date|Format|Admission
F["24_bring_back.txt"] = "\n".join([
    TAB.join(["Theater","City, ST","Title","Date","Format","Admission"]),
    TAB.join(["Paragon Village 12","Fredericksburg, VA","Animal Farm","5/1/2026","2D","1"]),
])

# 25. Caribbean / Puerto Rico
F["25_caribbean.txt"] = "\n".join([
    "APR 30'26 WK 18",
    TAB.join(["","Animal Farm"]),
    TAB.join(["Caribbean Cinemas Plaza Las Americas","1"]),
    TAB.join(["Caribbean Cinemas Montehiedra","combo"]),
])

# 26. IBS / Culbertson email comma-list + "opening FILM"
F["26_ibs_culbertson.txt"] = ("Jerseyville, Cinema 6, Highland Cinema, "
    "Litchfield Sky View opening Animal Farm")

# 27. Email prose "on FILM:" + theatres
F["27_email_prose.txt"] = "\n".join([
    "Hi team,","Please confirm the following on Animal Farm:",
    "Regal Downtown 12","Regal Riverside 8","Thanks,",
])

# 28. THEATRE single-header + alternating name/action
F["28_theatre_single_header.txt"] = "\n".join([
    "Animal Farm","THEATRE",
    "Cinemark 12 (Rosenberg, TX)","Final",
    "Cinemark 14 (Round Rock, TX)","Hold",
])

# 29. Clark CFB PDF holdover (CITY, ST headers + rows)
F["29_clark_cfb.txt"] = "\n".join([
    "SHERIDAN, WY",
    "Centennial Theater Animal Farm ANG 2D 2 5 3/15 1200 H * Yes",
    "STERLING, CO",
    "Fox 5 Theater Animal Farm ANG 2D 2 6 3/15 900 F * ",
])

# 30. AMC split-screen holdover variant
F["30_amc_split.txt"] = "\n".join([
    "AMC Film Programmer","Animal Farm",
    "DALLAS  AMC NorthPark 15  476 Split screen. Final",
    "DALLAS  AMC Mesquite 30  0 Opening - 04/30/2026",
])

for name, content in F.items():
    io.open(os.path.join(D, name), "w", encoding="utf-8", newline="\n").write(content)
print(f"wrote {len(F)} fixtures to {D}")
