-- ***************************************************************************--
--                   SÉBASTIAN OLAVE & LILIANA TAPIA
-- ***************************************************************************--

-- ***************************************************************************--
--                                CASO 1                                      --
-- ***************************************************************************--

-- DEFINICIÓN DE VARIABLES BIND (PARÁMETROS)
VARIABLE p_anio_anterior NUMBER
EXEC :p_anio_anterior := EXTRACT(YEAR FROM ADD_MONTHS(SYSDATE, -12));

VARIABLE p_rango1_inf NUMBER
VARIABLE p_rango1_sup NUMBER
VARIABLE p_rango2_inf NUMBER
VARIABLE p_rango2_sup NUMBER
VARIABLE p_rango3_inf NUMBER

EXEC :p_rango1_inf :=  500000;
EXEC :p_rango1_sup :=  700000;
EXEC :p_rango2_inf :=  700001;
EXEC :p_rango2_sup :=  900000;
EXEC :p_rango3_inf :=  900001;

-------------------------------------------------------------------------------
-- BLOQUE PL/SQL ANÓNIMO
-------------------------------------------------------------------------------
DECLARE
  -----------------------------------------------------------------------------
  -- VARRAY para los puntos (base + extras)
  -----------------------------------------------------------------------------
  TYPE t_puntos IS VARRAY(4) OF NUMBER; 
  v_puntos t_puntos := t_puntos(250, 300, 550, 700);
  -- v_puntos(1)=250 -> Puntos base
  -- v_puntos(2)=300 -> Extra Rango 1
  -- v_puntos(3)=550 -> Extra Rango 2
  -- v_puntos(4)=700 -> Extra Rango 3

  -----------------------------------------------------------------------------
  -- Cursor SIN parámetro para DETALLE
  -----------------------------------------------------------------------------
  CURSOR c_detalle_cur IS
    SELECT
      cli.numrun,
      cli.dvrun,
      ------------------------------ NUEVO: traemos el tipo de cliente
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

  -----------------------------------------------------------------------------
  -- Cursor CON parámetro para RESUMEN
  -----------------------------------------------------------------------------
  CURSOR c_resumen_cur(p_year NUMBER) IS
    SELECT
      TO_CHAR(ttc.fecha_transaccion,'MMYYY') AS mes_anno,
      SUM(CASE WHEN ttt.nombre_tptran_tarjeta LIKE 'Compra%' THEN ttc.monto_transaccion ELSE 0 END) AS monto_total_compras,
      0 AS total_puntos_compras,
      SUM(CASE WHEN ttt.nombre_tptran_tarjeta = 'Avance en Efectivo' THEN ttc.monto_transaccion ELSE 0 END) AS monto_total_avances,
      0 AS total_puntos_avances,
      SUM(CASE WHEN ttt.nombre_tptran_tarjeta LIKE 'Súper Avance%' THEN ttc.monto_transaccion ELSE 0 END) AS monto_total_savances,
      0 AS total_puntos_savances
    FROM transaccion_tarjeta_cliente ttc
         JOIN tipo_transaccion_tarjeta ttt
           ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = p_year
    GROUP BY TO_CHAR(ttc.fecha_transaccion,'MMYYY')
    ORDER BY TO_CHAR(ttc.fecha_transaccion,'MMYYY');

  -----------------------------------------------------------------------------
  -- Variables de tipo %ROWTYPE
  -----------------------------------------------------------------------------
  v_detalle  c_detalle_cur%ROWTYPE;
  v_resumen  c_resumen_cur%ROWTYPE;

  -----------------------------------------------------------------------------
  -- Variables auxiliares para el cálculo de puntos
  -----------------------------------------------------------------------------
  v_factor_100k    NUMBER := 0;
  v_base_points    NUMBER := 0;
  v_extra_points   NUMBER := 0;

BEGIN
  -----------------------------------------------------------------------------
  -- Truncamos las tablas de salida para permitir re-ejecución
  -----------------------------------------------------------------------------
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';

  -----------------------------------------------------------------------------
  -- PROCESAR DETALLE (Cursor sin parámetro)
  -----------------------------------------------------------------------------
  OPEN c_detalle_cur;
  LOOP
    FETCH c_detalle_cur INTO v_detalle;  
    EXIT WHEN c_detalle_cur%NOTFOUND;

    -- 1) Cálculo de puntos base
    v_factor_100k := TRUNC(v_detalle.monto_transaccion / 100000);
    v_base_points := v_factor_100k * v_puntos(1);  -- 250
    v_extra_points := 0;

    -- 2) SOLO si el COD_TIPO_CLIENTE es 30 (dueña de casa) O 40 (pensionado),
    --    aplicamos la lógica de rangos extras:
    IF v_detalle.cod_tipo_cliente IN (30, 40) THEN
       IF v_detalle.monto_transaccion BETWEEN :p_rango1_inf AND :p_rango1_sup THEN
          v_extra_points := v_factor_100k * v_puntos(2);  -- 300
       ELSIF v_detalle.monto_transaccion BETWEEN :p_rango2_inf AND :p_rango2_sup THEN
          v_extra_points := v_factor_100k * v_puntos(3);  -- 550
       ELSIF v_detalle.monto_transaccion >= :p_rango3_inf THEN
          v_extra_points := v_factor_100k * v_puntos(4);  -- 700
       END IF;
    END IF;

    -- 3) Suma final
    v_detalle.puntos_allthebest := v_base_points + v_extra_points;

    -- 4) Insertar en DETALLE_PUNTOS_TARJETA_CATB
    INSERT INTO detalle_puntos_tarjeta_catb (
      numrun,
      dvrun,
      nro_tarjeta,
      nro_transaccion,
      fecha_transaccion,
      tipo_transaccion,
      monto_transaccion,
      puntos_allthebest
    )
    VALUES (
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

  -----------------------------------------------------------------------------
  -- PROCESAR RESUMEN (Cursor con parámetro)
  -----------------------------------------------------------------------------
  OPEN c_resumen_cur(:p_anio_anterior);
  LOOP
    FETCH c_resumen_cur INTO v_resumen;  
    EXIT WHEN c_resumen_cur%NOTFOUND;

    -- Puntos base para COMPRAS
    v_factor_100k := TRUNC(v_resumen.monto_total_compras / 100000);
    v_base_points := v_factor_100k * v_puntos(1);
    v_extra_points := 0;
    v_resumen.total_puntos_compras := v_base_points + v_extra_points;

    -- Puntos base para AVANCES
    v_factor_100k := TRUNC(v_resumen.monto_total_avances / 100000);
    v_base_points := v_factor_100k * v_puntos(1);
    v_extra_points := 0;
    v_resumen.total_puntos_avances := v_base_points + v_extra_points;

    -- Puntos base para SÚPER AVANCES
    v_factor_100k := TRUNC(v_resumen.monto_total_savances / 100000);
    v_base_points := v_factor_100k * v_puntos(1);
    v_extra_points := 0;
    v_resumen.total_puntos_savances := v_base_points + v_extra_points;

    -- Insertar en RESUMEN_PUNTOS_TARJETA_CATB
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
      v_resumen.mes_anno,
      v_resumen.monto_total_compras,
      v_resumen.total_puntos_compras,
      v_resumen.monto_total_avances,
      v_resumen.total_puntos_avances,
      v_resumen.monto_total_savances,
      v_resumen.total_puntos_savances
    );
  END LOOP;
  CLOSE c_resumen_cur;

  DBMS_OUTPUT.PUT_LINE('Proceso finalizado OK para año anterior=' || :p_anio_anterior);

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error en el proceso: ' || SQLERRM);
    ROLLBACK;
END;
/

SELECT * FROM detalle_puntos_tarjeta_catb;
SELECT * FROM resumen_puntos_tarjeta_catb;



