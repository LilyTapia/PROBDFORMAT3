-- ***************************************************************************--
--                   SÉBASTIAN OLAVE & LILIANA TAPIA
-- ***************************************************************************--

-- ***************************************************************************--
--                                CASO 1                                      --
-- ***************************************************************************--

/*------------------------------------------------------------
  1) DEFINICIÓN DE VARIABLES BIND (PARÁMETROS)
------------------------------------------------------------*/
VARIABLE p_anio_anterior NUMBER;
BEGIN
  :p_anio_anterior := EXTRACT(YEAR FROM ADD_MONTHS(SYSDATE, -12));
END;
/
 
VARIABLE p_rango1_inf NUMBER;
BEGIN
  :p_rango1_inf := 500000;
END;
/
 
VARIABLE p_rango1_sup NUMBER;
BEGIN
  :p_rango1_sup := 700000;
END;
/
 
VARIABLE p_rango2_inf NUMBER;
BEGIN
  :p_rango2_inf := 700001;
END;
/
 
VARIABLE p_rango2_sup NUMBER;
BEGIN
  :p_rango2_sup := 900000;
END;
/
 
VARIABLE p_rango3_inf NUMBER;
BEGIN
  :p_rango3_inf := 900001;
END;
/
 
/*------------------------------------------------------------
  2) BLOQUE PL/SQL ANÓNIMO CON 2 CURSORES
     - Cursor SIN parámetro (detalle)
     - Cursor CON parámetro (resumen)
     - Proceso de cálculo y llenado de tablas
------------------------------------------------------------*/
DECLARE
  ------------------------------------------------------------
  -- (a) VARRAY para los puntos (base y extras)
  ------------------------------------------------------------
  TYPE t_puntos IS VARRAY(4) OF NUMBER; 
  v_puntos t_puntos := t_puntos(250, 300, 550, 700);
  -- v_puntos(1)=250 -> Puntos base
  -- v_puntos(2)=300 -> Extra Rango 1
  -- v_puntos(3)=550 -> Extra Rango 2
  -- v_puntos(4)=700 -> Extra Rango 3
  
  ------------------------------------------------------------
  -- (b) Cursor SIN parámetro (Variable de Cursor) para DETALLE
  ------------------------------------------------------------
  CURSOR c_detalle_cur IS
    SELECT
      cli.numrun,
      cli.dvrun,
      cli.cod_tipo_cliente, 
      ttc.nro_tarjeta,
      ttc.nro_transaccion,
      ttc.fecha_transaccion,
      ttt.nombre_tptran_tarjeta AS tipo_transaccion,
      ttc.monto_transaccion,
      0 AS puntos_allthebest
    FROM transaccion_tarjeta_cliente ttc
         JOIN tarjeta_cliente tc
           ON ttc.nro_tarjeta = tc.nro_tarjeta
         JOIN cliente cli
           ON tc.numrun = cli.numrun
         JOIN tipo_transaccion_tarjeta ttt
           ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = :p_anio_anterior
    ORDER BY ttc.fecha_transaccion, cli.numrun, ttc.nro_transaccion;
  
  ------------------------------------------------------------
  -- (c) Cursor CON parámetro para RESUMEN
  ------------------------------------------------------------
  CURSOR c_resumen_cur (p_year NUMBER) IS
    SELECT p_year AS anio_filtrado
      FROM DUAL;
    -- En este SELECT no recalculamos aquí los montos y puntos,
    -- sino que más abajo haremos un FOR que lee DETALLE para agrupar.
  ------------------------------------------------------------
  -- (d) Variables %ROWTYPE y auxiliares de cálculo
  ------------------------------------------------------------
  v_detalle   c_detalle_cur%ROWTYPE;
  v_dummy     c_resumen_cur%ROWTYPE;   -- Para leer desde el cursor con parámetro
  
  v_factor_100k   NUMBER := 0;
  v_base_points   NUMBER := 0;
  v_extra_points  NUMBER := 0;
  
BEGIN
  /*----------------------------------------------------------
    (1) Truncamos las tablas de salida antes de recargar
  ----------------------------------------------------------*/
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';
  
  /*----------------------------------------------------------
    (2) LLENAR DETALLE (cursor sin parámetro):
        - Calcula puntos (base + extras)
        - Inserta en DETALLE_PUNTOS_TARJETA_CATB
  ----------------------------------------------------------*/
  OPEN c_detalle_cur;
  LOOP
    FETCH c_detalle_cur INTO v_detalle;  
    EXIT WHEN c_detalle_cur%NOTFOUND;
 
    -- (2a) Calcular factor en múltiplos de 100.000
    v_factor_100k := TRUNC(v_detalle.monto_transaccion / 100000);
    
    -- (2b) Puntos base
    v_base_points := v_factor_100k * v_puntos(1);  -- 250 por cada 100.000
    
    -- (2c) Puntos extra para tipo de cliente 30 o 40, si cae en los rangos
    v_extra_points := 0;
    IF v_detalle.cod_tipo_cliente IN (30, 40) THEN
       IF v_detalle.monto_transaccion BETWEEN :p_rango1_inf AND :p_rango1_sup THEN
          v_extra_points := v_factor_100k * v_puntos(2);  -- 300
       ELSIF v_detalle.monto_transaccion BETWEEN :p_rango2_inf AND :p_rango2_sup THEN
          v_extra_points := v_factor_100k * v_puntos(3);  -- 550
       ELSIF v_detalle.monto_transaccion >= :p_rango3_inf THEN
          v_extra_points := v_factor_100k * v_puntos(4);  -- 700
       END IF;
    END IF;
    
    -- (2d) Total de puntos para la transacción
    v_detalle.puntos_allthebest := v_base_points + v_extra_points;
    
    -- (2e) Insertar registro en tabla DETALLE
    INSERT INTO detalle_puntos_tarjeta_catb (
      numrun,
      dvrun,
      nro_tarjeta,
      nro_transaccion,
      fecha_transaccion,
      tipo_transaccion,
      monto_transaccion,
      puntos_allthebest
    ) VALUES (
      v_detalle.numrun,
      v_detalle.dvrun,
      v_detalle.nro_tarjeta,
      v_detalle.nro_transaccion,
      v_detalle.fecha_transaccion,
      v_detalle.tipo_transaccion,
      v_detalle.monto_transaccion,
      v_detalle.puntos_allthebest
    );
  END LOOP;
  CLOSE c_detalle_cur;
  
  /*----------------------------------------------------------
    (3) PROCESAR RESUMEN:
        - Abrimos el cursor con parámetro c_resumen_cur
          para “usar” p_year (:p_anio_anterior).
        - Hacemos un FETCH.
        - Luego, en un FOR, agrupamos la tabla DETALLE y
          sumamos montos/puntos por mes.
  ----------------------------------------------------------*/
  OPEN c_resumen_cur(:p_anio_anterior);
  FETCH c_resumen_cur INTO v_dummy;  
  CLOSE c_resumen_cur;
  
  /*----------------------------------------------------------
    (3a) LLENAR RESUMEN DESDE DETALLE
         - Sumamos montos y puntos, agrupando por mes
  ----------------------------------------------------------*/
  FOR reg_mes IN (
    SELECT DISTINCT TO_CHAR(fecha_transaccion, 'MMYYYY') AS mes_anno
      FROM detalle_puntos_tarjeta_catb
     WHERE EXTRACT(YEAR FROM fecha_transaccion) = :p_anio_anterior
     ORDER BY TO_CHAR(fecha_transaccion,'MMYYYY')
  )
  LOOP
    DECLARE
      v_monto_compras      NUMBER := 0;
      v_puntos_compras     NUMBER := 0;
      v_monto_avances      NUMBER := 0;
      v_puntos_avances     NUMBER := 0;
      v_monto_savances     NUMBER := 0;
      v_puntos_savances    NUMBER := 0;
    BEGIN
      SELECT
        SUM(CASE WHEN tipo_transaccion LIKE 'Compras%' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE 'Compras%' THEN puntos_allthebest  ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion = 'Avance en Efectivo' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion = 'Avance en Efectivo' THEN puntos_allthebest  ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE 'S�per Avance%' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE 'S�per Avance%' THEN puntos_allthebest  ELSE 0 END)
      INTO
        v_monto_compras,
        v_puntos_compras,
        v_monto_avances,
        v_puntos_avances,
        v_monto_savances,
        v_puntos_savances
      FROM detalle_puntos_tarjeta_catb
      WHERE TO_CHAR(fecha_transaccion, 'MMYYYY') = reg_mes.mes_anno;
      
      INSERT INTO resumen_puntos_tarjeta_catb (
        mes_anno,
        monto_total_compras,
        total_puntos_compras,
        monto_total_avances,
        total_puntos_avances,
        monto_total_savances,
        total_puntos_savances
      )
      VALUES (
        reg_mes.mes_anno,
        v_monto_compras,
        v_puntos_compras,
        v_monto_avances,
        v_puntos_avances,
        v_monto_savances,
        v_puntos_savances
      );
    END;
  END LOOP;
  
  /*----------------------------------------------------------
    (4) COMMIT final y mensaje
  ----------------------------------------------------------*/
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Proceso finalizado OK para año anterior=' || :p_anio_anterior);

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error en el proceso: ' || SQLERRM);
    ROLLBACK;
END;
/
 
/*------------------------------------------------------------
  3) CONSULTAS DE VALIDACIÓN FINAL
------------------------------------------------------------*/


SELECT * 
FROM detalle_puntos_tarjeta_catb;
SELECT * 
FROM resumen_puntos_tarjeta_catb;




   
     
