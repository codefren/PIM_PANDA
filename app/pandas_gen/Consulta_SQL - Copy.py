consulta='''

SELECT 
    -- Producto
    d.fldidmarca AS p_brand,
    a.fldidarticulo AS p_nr,
    j.flddescripcion AS p_cluster_0,
    b.flddescripcion AS p_name,
    b.flddescripcion AS p_name_keyword,
    b.flddescripcion AS p_name_proper,
    c.fldDescriAmpli AS p_text,
    l.flddescripcion AS p_comp_composition,
    m.flddescripcion AS p_comp_care,
    d.fldIdSexo AS p_tag_gender,
    d.fldidedad AS p_tag_ageGroup,
    'All-season Basic Article' AS p_tag_season,
    ' [' + K.flddescriCorta + ']' AS p_tag_sizeGrid,

    -- Artículo
    a.fldidarticulo + '-' + a.fldidcolor + '-' + a.fldIdTalla AS a_nr,
    a.fldidcodigo AS a_ean,
    a.fldidarticulo + '-' + a.fldidcolor AS a_prodnr,
    a.fldIdTalla AS a_comp_size,
    e.fldDescripcion AS a_comp_brandcolour,
    e.fldDescripcion AS a_tag_colour,

    -- Imágenes
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_1.jpg' AS a_media_image_0,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_2.jpg' AS a_media_image_1,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_3.jpg' AS a_media_image_2,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_4.jpg' AS a_media_image_3,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_5.jpg' AS a_media_image_4,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_6.jpg' AS a_media_image_5,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_7.jpg' AS a_media_image_6,
    'sync/media/' + a.fldidarticulo + '_' + a.fldidcolor + '_8.jpg' AS a_media_image_7,

    -- Estado activo
    '1' AS a_active,

    -- Canales
    '1' AS a_active_CHANNEL1, ISNULL(f.fldtalla1, 0) AS a_vk_CHANNEL1, ISNULL(f.fldtalla1, 0) AS a_vk_old_CHANNEL1,
    '1' AS a_active_CHANNEL2, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL2, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL2,
    '1' AS a_active_CHANNEL3, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL3, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL3,
    '1' AS a_active_CHANNEL4, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL4, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL4,
    '1' AS a_active_CHANNEL5, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL5, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL5,
    '1' AS a_active_CHANNEL6, ISNULL(f5.fldtalla1, 0) AS a_vk_CHANNEL6, ISNULL(f5.fldtalla1, 0) AS a_vk_old_CHANNEL6,
    '1' AS a_active_CHANNEL7, ISNULL(f.fldtalla1, 0) AS a_vk_CHANNEL7, ISNULL(f.fldtalla1, 0) AS a_vk_old_CHANNEL7,
    '1' AS a_active_CHANNEL8, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL8, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL8,
    '1' AS a_active_CHANNEL9, ISNULL(f8.fldtalla1, 0) AS a_vk_CHANNEL9, ISNULL(f8.fldtalla1, 0) AS a_vk_old_CHANNEL9,
    '1' AS a_active_CHANNEL10, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL10, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL10,
    '1' AS a_active_CHANNEL11, ISNULL(f23.fldtalla1, 0) AS a_vk_CHANNEL11, ISNULL(f23.fldtalla1, 0) AS a_vk_old_CHANNEL11,
    '1' AS a_active_CHANNEL12, ISNULL(f11.fldtalla1, 0) AS a_vk_CHANNEL12, ISNULL(f11.fldtalla1, 0) AS a_vk_old_CHANNEL12,
    '1' AS a_active_CHANNEL13, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL13, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL13,
    '1' AS a_active_CHANNEL14, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL14, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL14,
    '1' AS a_active_CHANNEL15, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL15, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL15,
    '1' AS a_active_CHANNEL16, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL16, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL16,
    '1' AS a_active_CHANNEL17, ISNULL(f16.fldtalla1, 0) AS a_vk_CHANNEL17, ISNULL(f16.fldtalla1, 0) AS a_vk_old_CHANNEL17,
    '1' AS a_active_CHANNEL18, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL18, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL18,
    '1' AS a_active_CHANNEL19, ISNULL(f22.fldtalla1, 0) AS a_vk_CHANNEL19, ISNULL(f22.fldtalla1, 0) AS a_vk_old_CHANNEL19,
    '1' AS a_active_CHANNEL20, ISNULL(f.fldtalla1, 0) AS a_vk_CHANNEL20, ISNULL(f.fldtalla1, 0) AS a_vk_old_CHANNEL20,
    '1' AS a_active_CHANNEL21, ISNULL(f20.fldtalla1, 0) AS a_vk_CHANNEL21, ISNULL(f20.fldtalla1, 0) AS a_vk_old_CHANNEL21,
    '1' AS a_active_CHANNEL22, ISNULL(f21.fldtalla1, 0) AS a_vk_CHANNEL22, ISNULL(f21.fldtalla1, 0) AS a_vk_old_CHANNEL22,
    '1' AS a_active_CHANNEL23, ISNULL(f24.fldtalla1, 0) AS a_vk_CHANNEL23, ISNULL(f24.fldtalla1, 0) AS a_vk_old_CHANNEL23,
    '1' AS a_active_CHANNEL24, ISNULL(f25.fldtalla1, 0) AS a_vk_CHANNEL24, ISNULL(f25.fldtalla1, 0) AS a_vk_old_CHANNEL24,
    '1' AS a_active_CHANNEL25, ISNULL(f26.fldtalla1, 0) AS a_vk_CHANNEL25, ISNULL(f26.fldtalla1, 0) AS a_vk_old_CHANNEL25,
    '1' AS a_active_CHANNEL26, ISNULL(f25.fldtalla1, 0) AS a_vk_CHANNEL26, ISNULL(f25.fldtalla1, 0) AS a_vk_old_CHANNEL26,
    '1' AS a_active_CHANNEL27, ISNULL(f25.fldtalla1, 0) AS a_vk_CHANNEL27, ISNULL(f25.fldtalla1, 0) AS a_vk_old_CHANNEL27,
    '1' AS a_active_CHANNEL26, ISNULL(f.fldtalla1, 0) AS a_vk_CHANNEL28, ISNULL(f.fldtalla1, 0) AS a_vk_old_CHANNEL28,

    -- Otros
    '2' AS a_stock,
    n.fldPais AS a_org_country,
    c.fldIdPartidaArancel AS a_intrastat,

    -- Bullets
    bu.fldBullet1, bu.fldBullet2, bu.fldBullet3, bu.fldBullet4, bu.fldBullet5, bu.fldBullet6,
    bu.fldBullet7, bu.fldBullet8, bu.fldBullet9, bu.fldBullet10, bu.fldBullet11, bu.fldBullet12,
    bu.fldBullet13, bu.fldBullet14, bu.fldBullet15,

    -- Composición extendida
    co.fldaymaterial, co.fldFaux_fur_collar_material, co.fldfilling, co.fldinner_jacket_lining,
    co.fldinner_jacket_padding, co.fldinsertMaterial, co.fldlining, co.fldupper_material_sleeves,

    b.fldcantidadcaja,
    c.fldpeso

-- FROM y JOINs se mantienen igual que el ejemplo anterior, para no cortar la ejecución

FROM zzTempArticuloColor aa
INNER JOIN tbdArticulosBarrasAECOC_2 a ON aa.fldidarticulo = a.fldidarticulo AND aa.fldidcolor = a.fldidcolor
INNER JOIN tbdArticulos0 b ON a.fldIdArticulo = b.fldIdArticulo
INNER JOIN tbdArticulos1 c ON a.fldIdArticulo = c.fldIdArticulo
INNER JOIN tbdarticulos5 d ON a.fldIdArticulo = d.fldidarticulo
INNER JOIN tbdColores e ON a.fldIdColor = e.fldIdColor
INNER JOIN tbdArticulosTarifas f ON a.fldIdArticulo = f.fldIdArticulo AND f.fldIdTarifa = '11'
INNER JOIN tbdArticulosTarifas f5 ON a.fldIdArticulo = f5.fldIdArticulo AND f5.fldIdTarifa = '60'
INNER JOIN tbdArticulosTarifas f8 ON a.fldIdArticulo = f8.fldIdArticulo AND f8.fldIdTarifa = '61'
INNER JOIN tbdArticulosTarifas f11 ON a.fldIdArticulo = f11.fldIdArticulo AND f11.fldIdTarifa = '63'
INNER JOIN tbdArticulosTarifas f16 ON a.fldIdArticulo = f16.fldIdArticulo AND f16.fldIdTarifa = '62'
INNER JOIN tbdArticulosTarifas f20 ON a.fldIdArticulo = f20.fldIdArticulo AND f20.fldIdTarifa = '65'
INNER JOIN tbdArticulosTarifas f21 ON a.fldIdArticulo = f21.fldIdArticulo AND f21.fldIdTarifa = '14'
INNER JOIN tbdArticulosTarifas f22 ON a.fldIdArticulo = f22.fldIdArticulo AND f22.fldIdTarifa = '12'
INNER JOIN tbdArticulosTarifas f23 ON a.fldIdArticulo = f23.fldIdArticulo AND f23.fldIdTarifa = '13'
INNER JOIN tbdArticulosTarifas f24 ON a.fldIdArticulo = f24.fldIdArticulo AND f24.fldIdTarifa = '66'
INNER JOIN tbdArticulosTarifas f25 ON a.fldIdArticulo = f25.fldIdArticulo AND f25.fldIdTarifa = '67'
INNER JOIN tbdArticulosTarifas f26 ON a.fldIdArticulo = f26.fldIdArticulo AND f26.fldIdTarifa = '68'
INNER JOIN tbdFamilias j ON d.fldIdFamilia = j.fldidfamilia
INNER JOIN tbdproveedores i ON b.fldIdProveedor = i.fldIdProveedor
INNER JOIN tbdproveedores n ON b.fldIdProveedor = n.fldIdProveedor
INNER JOIN tbdgrupostallasPaises k ON b.fldIdGrupoTallas = k.fldIdGrupoTallas AND k.fldIdPais = '00'
INNER JOIN tbdComposiciones l ON c.fldIdComposicion = l.fldIdComposicion
INNER JOIN tbdComposiciones m ON c.fldIdComposicionPiel = m.fldIdComposicion
INNER JOIN tbdarticulosWebBullet bu ON bu.fldidarticulo = a.fldidarticulo AND bu.fldIdColor = e.fldIdColor AND bu.fldidioma = 'UK'
INNER JOIN tbdarticulosWebCompo co ON co.fldidarticulo = a.fldidarticulo AND co.fldIdColor = e.fldIdColor AND co.fldidioma = 'UK'

WHERE 
    LEFT(a.fldidcodigo, 2) = '84'
    AND a.fldPosicionTalla > 0

ORDER BY 
    a.fldidarticulo,
    a.fldIdColor,
    a.fldPosicionTalla;

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
    "a_active_CHANNEL23", "a_vk_CHANNEL23", "a_vk_old_CHANNEL23",
    "a_active_CHANNEL24", "a_vk_CHANNEL24", "a_vk_old_CHANNEL24",
    "a_active_CHANNEL25", "a_vk_CHANNEL25", "a_vk_old_CHANNEL25",
    "a_active_CHANNEL26", "a_vk_CHANNEL26", "a_vk_old_CHANNEL26",
    "a_active_CHANNEL27", "a_vk_CHANNEL27", "a_vk_old_CHANNEL27",
    "a_active_CHANNEL28", "a_vk_CHANNEL28", "a_vk_old_CHANNEL28",
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
    "a_active[fbcno]", "a_vk[fbcno]", "a_vk_old[fbcno]",
    "a_active[fbccd]", "a_vk[fbccd]", "a_vk_old[fbccd]",
    "a_active[fbcbg]", "a_vk[fbcbg]", "a_vk_old[fbcbg]",
    "a_active[zazfscd]", "a_vk[zazfscd]", "a_vk_old[zazfscd]",
    "a_active[zazfscf]", "a_vk[zazfscf]", "a_vk_old[zazfscf]",
    "a_active[zalandomain]", "a_vk[zalandomain]", "a_vk_old[zalandomain]",
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
