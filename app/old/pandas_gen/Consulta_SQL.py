consulta='''
select d.fldidmarca as p_brand, a.fldidarticulo as p_nr, j.flddescripcion as p_cluster_0, b.flddescripcion as p_name, 
b.flddescripcion as p_name_keyword, b.flddescripcion as p_name_proper, c.fldDescriAmpli as p_text, 
l.flddescripcion as p_comp_composition, m.flddescripcion as p_comp_care, d.fldIdSexo as p_tag_gender, 
d.fldidedad as p_tag_ageGroup, 'All-season Basic Article' as p_tag_season,  ' [' + K.flddescriCorta + ']' as p_tag_sizeGrid,  
a.fldidarticulo + '-' + a.fldidcolor + '-' + a.fldIdTalla as a_nr, a.fldidcodigo as a_ean, 
a.fldidarticulo + '-' + a.fldidcolor as a_prodnr, a.fldIdTalla as a_comp_size, e.fldDescripcion as a_comp_brandcolour, 
e.fldDescripcion as a_tag_colour, 'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_1.jpg' as a_media_image_0, 
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_2.jpg' as a_media_image_1, 
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_3.jpg' as a_media_image_2,  
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_4.jpg' as a_media_image_3, 
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_5.jpg' as a_media_image_4, 
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_6.jpg' as a_media_image_5,  
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_7.jpg' as a_media_image_6, 
'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_8.jpg' as a_media_image_7, '1' as a_active,  
'1' as a_active_CHANNEL1, f.fldtalla1 as a_vk_CHANNEL1, f.fldtalla1 as a_vk_old_CHANNEL1,   
'1' as a_active_CHANNEL2, f22.fldtalla1 as a_vk_CHANNEL2, f22.fldtalla1 as a_vk_old_CHANNEL2,   
'1' as a_active_CHANNEL3, f22.fldtalla1 as a_vk_CHANNEL3, f22.fldtalla1 as a_vk_old_CHANNEL3,   
'1' as a_active_CHANNEL4, f22.fldtalla1 as a_vk_CHANNEL4, f22.fldtalla1 as a_vk_old_CHANNEL4,   
'1' as a_active_CHANNEL5, f22.fldtalla1 as a_vk_CHANNEL5, f22.fldtalla1 as a_vk_old_CHANNEL5,   
'1' as a_active_CHANNEL6, f5.fldtalla1 as a_vk_CHANNEL6, f5.fldtalla1 as a_vk_old_CHANNEL6,   
'1' as a_active_CHANNEL7, f.fldtalla1 as a_vk_CHANNEL7, f.fldtalla1 as a_vk_old_CHANNEL7,   
'1' as a_active_CHANNEL8, f22.fldtalla1 as a_vk_CHANNEL8, f22.fldtalla1 as a_vk_old_CHANNEL8,   
'1' as a_active_CHANNEL9, f8.fldtalla1 as a_vk_CHANNEL9, f8.fldtalla1 as a_vk_old_CHANNEL9,   
'1' as a_active_CHANNEL10, f22.fldtalla1 as a_vk_CHANNEL10, f22.fldtalla1 as a_vk_old_CHANNEL10,   
'1' as a_active_CHANNEL11, f23.fldtalla1 as a_vk_CHANNEL11, f23.fldtalla1 as a_vk_old_CHANNEL11,    
'1' as a_active_CHANNEL12, f11.fldtalla1 as a_vk_CHANNEL12, f11.fldtalla1 as a_vk_old_CHANNEL12,    
'1' as a_active_CHANNEL13, f22.fldtalla1 as a_vk_CHANNEL13, f22.fldtalla1 as a_vk_old_CHANNEL13,    
'1' as a_active_CHANNEL14, f22.fldtalla1 as a_vk_CHANNEL14, f22.fldtalla1 as a_vk_old_CHANNEL14,    
'1' as a_active_CHANNEL15, f22.fldtalla1 as a_vk_CHANNEL15, f22.fldtalla1 as a_vk_old_CHANNEL15,    
'1' as a_active_CHANNEL16, f22.fldtalla1 as a_vk_CHANNEL16, f22.fldtalla1 as a_vk_old_CHANNEL16,    
'1' as a_active_CHANNEL17, f16.fldtalla1 as a_vk_CHANNEL17, f16.fldtalla1 as a_vk_old_CHANNEL17,    
'1' as a_active_CHANNEL18, f22.fldtalla1 as a_vk_CHANNEL18, f22.fldtalla1 as a_vk_old_CHANNEL18,    
'1' as a_active_CHANNEL19, f22.fldtalla1 as a_vk_CHANNEL19, f22.fldtalla1 as a_vk_old_CHANNEL19,    
'1' as a_active_CHANNEL20, f.fldtalla1 as a_vk_CHANNEL20, f.fldtalla1 as a_vk_old_CHANNEL20,    
'1' as a_active_CHANNEL21, f20.fldtalla1 as a_vk_CHANNEL21, f20.fldtalla1 as a_vk_old_CHANNEL21,    
'1' as a_active_CHANNEL22, f21.fldtalla1 as a_vk_CHANNEL22, f21.fldtalla1 as a_vk_old_CHANNEL22,    
'2' as a_stock, n.fldPais as a_org_country, c.fldIdPartidaArancel as a_intrastat, 
bu.fldBullet1, bu.fldBullet2, bu.fldBullet3, bu.fldBullet4, bu.fldBullet5, bu.fldBullet6,
bu.fldBullet7, bu.fldBullet8, bu.fldBullet9, bu.fldBullet10, bu.fldBullet11, bu.fldBullet12, 
bu.fldBullet13, bu.fldBullet14, bu.fldBullet15, 
co.fldaymaterial, co.fldFaux_fur_collar_material, co.fldfilling, co.fldinner_jacket_lining, 
co.fldinner_jacket_padding, co.fldinsertMaterial, co.fldlining, co.fldupper_material_sleeves, b.fldcantidadcaja, c.fldpeso 
from zzTempArticuloColor aa, tbdArticulosBarrasAECOC_2 a, tbdArticulos0 b, tbdArticulos1 c, tbdarticulos5 d, tbdColores e, tbdArticulosTarifas f, tbdFamilias j, tbdproveedores i, tbdgrupostallasPaises k, tbdComposiciones l, tbdComposiciones m, tbdproveedores n, 
tbdArticulosTarifas f5, tbdArticulosTarifas f8, tbdArticulosTarifas f11, tbdArticulosTarifas f16, tbdArticulosTarifas f20, tbdArticulosTarifas f21 , tbdArticulosTarifas f22, tbdArticulosTarifas f23, tbdarticulosWebBullet bu, tbdarticulosWebCompo co  
where aa.fldidarticulo = a.fldidarticulo 
and aa.fldidcolor = a.fldidcolor 
and a.fldIdArticulo = b.fldIdArticulo 
and a.fldIdArticulo = c.fldIdArticulo 
and a.fldIdArticulo = d.fldidarticulo 
and a.fldIdColor = e.fldIdColor 
and d.fldIdFamilia = j.fldidfamilia 
and a.fldIdArticulo = f.fldIdArticulo   
and f.fldIdTarifa = '11' 
and a.fldIdArticulo = f5.fldIdArticulo   
and f5.fldIdTarifa = '60' 
and a.fldIdArticulo = f8.fldIdArticulo   
and f8.fldIdTarifa = '61' 
and a.fldIdArticulo = f11.fldIdArticulo  
and f11.fldIdTarifa = '63' 
and a.fldIdArticulo = f16.fldIdArticulo  
and f16.fldIdTarifa = '62' 
and a.fldIdArticulo = f20.fldIdArticulo  
and f20.fldIdTarifa = '65' 
and a.fldIdArticulo = f21.fldIdArticulo  
and f21.fldIdTarifa = '14'
and a.fldIdArticulo = f22.fldIdArticulo
and f22.fldIdTarifa = '12'
and a.fldIdArticulo = f23.fldIdArticulo
and f23.fldIdTarifa = '13'
and bu.fldidarticulo = a.fldidarticulo
and bu.fldIdColor = e.fldIdColor 
and bu.fldidioma = 'UK'
and co.fldidarticulo = a.fldidarticulo 
and co.fldIdColor = e.fldIdColor 
and co.fldidioma = 'UK' 
AND a.fldPosicionTalla > 0 
and b.fldIdProveedor = i.fldIdProveedor 
and b.fldIdGrupoTallas = k.fldIdGrupoTallas 
and k.fldIdPais = '00' 
and c.fldIdComposicion = l.fldIdComposicion 
and c.fldIdComposicionPiel = m.fldIdComposicion 
and b.fldIdProveedor = n.fldIdProveedor 
and left(a.fldidcodigo,2) = '84' 
order by a.fldidarticulo, a.fldIdColor, a.fldPosicionTalla 
'''

column_names_sql = [
    "p_brand", "p_nr", "p_cluster_0", "p_name",
    "p_name_keyword", "p_name_proper", "p_text",
    "p_comp_composition", "p_comp_care", "p_tag_gender",
    "p_tag_ageGroup", "p_tag_season", "p_tag_sizeGrid",
    "a_nr", "a_ean", "a_prodnr", "a_comp_size", "a_comp_brandcolour",
    "a_tag_colour", "a_media_image_0", "a_media_image_1",
    "a_media_image_2", "a_media_image_3", "a_media_image_4",
    "a_media_image_5", "a_media_image_6", "a_media_image_7", "a_active",
    "a_active_CHANNEL1", "a_vk_CHANNEL1", "a_vk_old_CHANNEL1",
    "a_active_CHANNEL2", "a_vk_CHANNEL2", "a_vk_old_CHANNEL2",
    "a_active_CHANNEL3", "a_vk_CHANNEL3", "a_vk_old_CHANNEL3",
    "a_active_CHANNEL4", "a_vk_CHANNEL4", "a_vk_old_CHANNEL4",
    "a_active_CHANNEL5", "a_vk_CHANNEL5", "a_vk_old_CHANNEL5",
    "a_active_CHANNEL6", "a_vk_CHANNEL6", "a_vk_old_CHANNEL6",
    "a_active_CHANNEL7", "a_vk_CHANNEL7", "a_vk_old_CHANNEL7",
    "a_active_CHANNEL8", "a_vk_CHANNEL8", "a_vk_old_CHANNEL8",
    "a_active_CHANNEL9", "a_vk_CHANNEL9", "a_vk_old_CHANNEL9",
    "a_active_CHANNEL10", "a_vk_CHANNEL10", "a_vk_old_CHANNEL10",
    "a_active_CHANNEL11", "a_vk_CHANNEL11", "a_vk_old_CHANNEL11",
    "a_active_CHANNEL12", "a_vk_CHANNEL12", "a_vk_old_CHANNEL12",
    "a_active_CHANNEL13", "a_vk_CHANNEL13", "a_vk_old_CHANNEL13",
    "a_active_CHANNEL14", "a_vk_CHANNEL14", "a_vk_old_CHANNEL14",
    "a_active_CHANNEL15", "a_vk_CHANNEL15", "a_vk_old_CHANNEL15",
    "a_active_CHANNEL16", "a_vk_CHANNEL16", "a_vk_old_CHANNEL16",
    "a_active_CHANNEL17", "a_vk_CHANNEL17", "a_vk_old_CHANNEL17",
    "a_active_CHANNEL18", "a_vk_CHANNEL18", "a_vk_old_CHANNEL18",
    "a_active_CHANNEL19", "a_vk_CHANNEL19", "a_vk_old_CHANNEL19",
    "a_active_CHANNEL20", "a_vk_CHANNEL20", "a_vk_old_CHANNEL20",
    "a_active_CHANNEL21", "a_vk_CHANNEL21", "a_vk_old_CHANNEL21",
    "a_active_CHANNEL22", "a_vk_CHANNEL22", "a_vk_old_CHANNEL22",
    "a_stock", "a_org_country", "a_intrastat",
    "fldBullet1", "fldBullet2", "fldBullet3", "fldBullet4", "fldBullet5", "fldBullet6",
    "fldBullet7", "fldBullet8", "fldBullet9", "fldBullet10", "fldBullet11", "fldBullet12",
    "fldBullet13", "fldBullet14", "fldBullet15",
    "fldaymaterial", "fldFaux_fur_collar_material", "fldfilling", "fldinner_jacket_lining",
    "fldinner_jacket_padding", "fldinsertMaterial", "fldlining", "fldupper_material_sleeves", "fldcantidadcaja", "fldpeso"
]

column_names_csv = [
    "p_brand", "p_nr", "p_cluster{0}", "p_name",
    "p_name_keyword", "p_name_proper", "p_text",
    "p_comp[composition]", "p_comp[care]", "p_tag[gender]",
    "p_tag[ageGroup]", "p_tag[season]", "p_tag[sizeGrid]",
    "a_nr", "a_ean", "a_prodnr", "a_comp[size]", "a_comp[brandcolour]",
    "a_tag[colour]", "a_media[image]{0}", "a_media[image]{1}",
    "a_media[image]{2}", "a_media[image]{3}", "a_media[image]{4}",
    "a_media[image]{5}", "a_media[image]{6}", "a_media[image]{7}",
    "a_active",
    "a_active[zafd]", "a_vk[zafd]", "a_vk_old[zafd]",
    "a_active[zafb]", "a_vk[zafb]", "a_vk_old[zafb]",
    "a_active[zafh]", "a_vk[zafh]", "a_vk_old[zafh]",
    "a_active[zaff]", "a_vk[zaff]", "a_vk_old[zaff]",
    "a_active[zafn]", "a_vk[zafn]", "a_vk_old[zafn]",
    "a_active[zazfspl]", "a_vk[zazfspl]", "a_vk_old[zazfspl]",
    "a_active[zafa]", "a_vk[zafa]", "a_vk_old[zafa]",
    "a_active[zazi]", "a_vk[zazi]", "a_vk_old[zazi]",
    "a_active[zazfsdk]", "a_vk[zazfsdk]", "a_vk_old[zazfsdk]",
    "a_active[zazfsfi]", "a_vk[zazfsfi]", "a_vk_old[zazfsfi]",
    "a_active[zafe]", "a_vk[zafe]", "a_vk_old[zafe]",
    "a_active[zazfscz]", "a_vk[zazfscz]", "a_vk_old[zazfscz]",
    "a_active[zazfsee]", "a_vk[zazfsee]", "a_vk_old[zazfsee]",
    "a_active[zazfshr]", "a_vk[zazfshr]", "a_vk_old[zazfshr]",
    "a_active[zazfslt]", "a_vk[zazfslt]", "a_vk_old[zazfslt]",
    "a_active[zazfslv]", "a_vk[zazfslv]", "a_vk_old[zazfslv]",
    "a_active[zazfsse]", "a_vk[zazfsse]", "a_vk_old[zazfsse]",
    "a_active[zazfssi]", "a_vk[zazfssi]", "a_vk_old[zazfssi]",
    "a_active[zazfssk]", "a_vk[zazfssk]", "a_vk_old[zazfssk]",
    "a_active[ayfd]", "a_vk[ayfd]", "a_vk_old[ayfd]",
    "a_active[zazfshu]", "a_vk[zazfshu]", "a_vk_old[zazfshu]",
    "a_active[zazfsro]", "a_vk[zazfsro]", "a_vk_old[zazfsro]",
    "a_stock", "a_org_country", "a_intrastat",
    "p_bullet{0}", "p_bullet{1}", "p_bullet{2}",
    "p_bullet{3}", "p_bullet{4}", "p_bullet{5}",
    "p_bullet{6}", "p_bullet{7}", "p_bullet{8}",
    "p_bullet{9}", "p_bullet{10}", "p_bullet{11}",
    "p_bullet{12}", "p_bullet{13}", "p_bullet{14}",
    "p_comp[Faux_fur_collar_material]", "p_comp[filling]",
    "p_comp[inner_jacket_lining]", "p_comp[inner_jacket_padding]",
    "p_comp[insertMaterial]", "p_comp[lining]",
    "p_comp[upper_material_sleeves]", "a_weight"
]
