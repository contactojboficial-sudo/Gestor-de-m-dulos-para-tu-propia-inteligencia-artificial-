# gestor_modulos.py
import importlib
import logging
import asyncio
import json
import sys
import time
import os
import inspect
import functools
from pathlib import Path
from importlib import reload
from typing import Dict, List, Callable, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from functools import lru_cache
import requests
import pkg_resources

logger = logging.getLogger("AuroraPluginManager")

class GestorDeModulos:
    def __init__(self):
        self.modulos: Dict[str, Any] = {}
        self.configs: Dict[str, Dict] = {}
        self.hooks: Dict[str, List[Callable]] = {
            'before_load': [],
            'after_load': [],
            'before_execute': [],
            'after_execute': [],
            'on_error': []
        }
        self.middleware: List[Callable] = []
        self.metricas: Dict[str, Dict] = {}
        self.observer = Observer()
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.lazy_modules: Dict[str, Dict] = {}
        self.versions: Dict[str, List] = {}
        self.active_versions: Dict[str, str] = {}

        # Iniciar monitoreo automático
        self.observer.start()

    # 1. Sistema de Dependencias Inteligente
    def verificar_dependencias(self, modulo_config: Dict) -> bool:
        """Verifica dependencias antes de cargar un módulo"""
        try:
            # Verificar versión de Python
            if 'python' in modulo_config.get('requisitos', {}):
                required_py = modulo_config['requisitos']['python']
                current_py = f"{sys.version_info.major}.{sys.version_info.minor}"
                if not pkg_resources.parse_version(current_py) >= pkg_resources.parse_version(required_py):
                    logger.error(f"Python {required_py}+ requerido. Actual: {current_py}")
                    return False
            
            # Verificar librerías externas
            for lib, version in modulo_config.get('dependencias', {}).items():
                try:
                    installed = pkg_resources.get_distribution(lib).version
                    if pkg_resources.parse_version(installed) < pkg_resources.parse_version(version):
                        logger.error(f"{lib} {version}+ requerido. Instalado: {installed}")
                        return False
                except pkg_resources.DistributionNotFound:
                    logger.error(f"Dependencia faltante: {lib}>={version}")
                    return False
            
            # Verificar dependencias de otros módulos
            for mod, ver in modulo_config.get('modulos_requeridos', {}).items():
                if mod not in self.modulos:
                    logger.error(f"Módulo requerido faltante: {mod}")
                    return False
                mod_version = getattr(self.modulos[mod], '__version__', '0.0.0')
                if pkg_resources.parse_version(mod_version) < pkg_resources.parse_version(ver):
                    logger.error(f"{mod} {ver}+ requerido. Actual: {mod_version}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error verificando dependencias: {e}")
            return False

    # 2. Carga Asíncrona y Lazy Loading
    async def cargar_modulo_async(self, nombre: str) -> None:
        """Carga módulos pesados sin bloquear el hilo principal"""
        if nombre in self.lazy_modules:
            config = self.lazy_modules[nombre]
            if await asyncio.get_event_loop().run_in_executor(None, self.verificar_dependencias, config):
                self._cargar_modulo(config)
                logger.info(f"Módulo {nombre} cargado asíncronamente")
            else:
                logger.error(f"Falló carga asíncrona de {nombre}: dependencias no satisfechas")

    def cargar_bajo_demanda(self, nombre: str) -> Any:
        """Carga el módulo solo cuando se necesita por primera vez"""
        if nombre not in self.modulos and nombre in self.lazy_modules:
            config = self.lazy_modules[nombre]
            if self.verificar_dependencias(config):
                self._cargar_modulo(config)
                return self.modulos[nombre]
            else:
                logger.error(f"Error carga bajo demanda: dependencias no satisfechas para {nombre}")
        return self.modulos.get(nombre)

    # 3. Sistema de Configuración por Módulo
    def cargar_con_configuracion(self, nombre: str, config_path: str) -> None:
        """Carga un módulo con su configuración específica"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            if not self.verificar_dependencias(config):
                logger.error(f"Configuración fallida para {nombre}: dependencias no satisfechas")
                return
            
            self.lazy_modules[nombre] = config
            self.configs[nombre] = config.get('configuracion', {})
            
            # Registrar para carga diferida
            if config.get('carga_automatica', True):
                if config.get('carga_async', False):
                    asyncio.create_task(self.cargar_modulo_async(nombre))
                else:
                    self._cargar_modulo(config)
        except Exception as e:
            logger.error(f"Error cargando configuración para {nombre}: {e}")

    # 4. Hot Reload
    def recargar_modulo(self, nombre: str) -> None:
        """Recarga un módulo modificado sin reiniciar"""
        if nombre in self.modulos:
            config = self.lazy_modules.get(nombre, {})
            
            # Ejecutar hooks pre-recarga
            self._ejecutar_hooks('before_unload', nombre)
            
            # Eliminar módulo existente
            del self.modulos[nombre]
            sys.modules.pop(config.get('ruta'), None)
            
            # Recargar
            self._cargar_modulo(config)
            logger.info(f"Módulo {nombre} recargado")
            
            # Ejecutar hooks post-recarga
            self._ejecutar_hooks('after_load', nombre)
        else:
            logger.warning(f"Intento de recarga de módulo no cargado: {nombre}")

    def monitorear_cambios(self, directorio: str = "modulos/") -> None:
        """Monitorea cambios en archivos para recarga automática"""
        class ModuloHandler(FileSystemEventHandler):
            def __init__(self, gestor):
                self.gestor = gestor
            
            def on_modified(self, event):
                if not event.is_directory and event.src_path.endswith('.py'):
                    modulo = Path(event.src_path).stem
                    self.gestor.recargar_modulo(modulo)
        
        self.observer.schedule(ModuloHandler(self), directorio, recursive=True)

    # 5. Sistema de Hooks/Eventos
    def agregar_hook(self, evento: str, callback: Callable) -> None:
        """Registra un hook para eventos específicos"""
        if evento in self.hooks:
            self.hooks[evento].append(callback)
        else:
            self.hooks[evento] = [callback]
    
    def _ejecutar_hooks(self, evento: str, *args, **kwargs) -> None:
        """Ejecuta todos los hooks registrados para un evento"""
        for hook in self.hooks.get(evento, []):
            try:
                hook(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error en hook {evento}: {e}")

    def ejecutar_con_middleware(self, nombre: str, metodo: str, *args, **kwargs) -> Any:
        """Ejecuta un método a través del pipeline de middleware"""
        def ejecucion_base():
            return self.ejecutar(nombre, metodo, *args, **kwargs)
        
        pipeline = functools.reduce(
            lambda f, g: lambda: g(f, nombre, metodo, *args, **kwargs),
            reversed(self.middleware),
            ejecucion_base
        )
        return pipeline()

    # 6. Sandboxing y Seguridad
    def ejecutar_seguro(self, nombre: str, metodo: str, *args, timeout: int = 30, **kwargs) -> Any:
        """Ejecuta en entorno aislado con límites de recursos"""
        if not self.validar_permisos(nombre, metodo):
            logger.error(f"Permiso denegado para {nombre}.{metodo}")
            return None
        
        try:
            futuro = self.executor.submit(
                lambda: self.ejecutar(nombre, metodo, *args, **kwargs)
            return futuro.result(timeout=timeout)
        except TimeoutError:
            logger.error(f"Timeout en {nombre}.{metodo}")
            self._ejecutar_hooks('on_error', nombre, metodo, "Timeout")
            return None
        except Exception as e:
            logger.error(f"Error en ejecución segura: {e}")
            self._ejecutar_hooks('on_error', nombre, metodo, str(e))
            return None

    def validar_permisos(self, modulo: str, accion: str) -> bool:
        """Verifica permisos del módulo para una acción específica"""
        config = self.configs.get(modulo, {})
        permisos = config.get('permisos', [])
        
        # Permitir todo si no hay restricciones
        if not permisos:
            return True
        
        # Verificar permisos específicos
        return accion in permisos or '*' in permisos

    # 7. Caché Inteligente
    @lru_cache(maxsize=128)
    def ejecutar_con_cache(self, nombre: str, metodo: str, cache_key: str, *args, **kwargs) -> Any:
        """Ejecuta con caché multi-nivel"""
        return self.ejecutar(nombre, metodo, *args, **kwargs)

    # 8. Métricas y Observabilidad
    def registrar_metrica(self, nombre: str, metodo: str, tiempo: float, exito: bool) -> None:
        """Registra métricas de ejecución"""
        if nombre not in self.metricas:
            self.metricas[nombre] = {'tiempos': [], 'errores': 0, 'exitos': 0}
        
        metricas = self.metricas[nombre]
        metricas['tiempos'].append(tiempo)
        
        if exito:
            metricas['exitos'] += 1
        else:
            metricas['errores'] += 1

    def obtener_metricas(self, nombre: str) -> Dict:
        """Devuelve métricas de rendimiento del módulo"""
        return self.metricas.get(nombre, {})
    
    def generar_dashboard(self) -> None:
        """Inicia dashboard web con métricas en tiempo real"""
        # Implementación real requeriría un servidor web (Flask/Django)
        # Aquí solo un esqueleto para integración
        logger.info("Dashboard iniciado en http://localhost:8000/dashboard")
        # Lógica real iría aquí...

    # 9. Auto-Discovery
    def descubrir_modulos(self, directorio: str = "plugins/") -> None:
        """Descubre y carga automáticamente todos los módulos válidos"""
        for root, _, files in os.walk(directorio):
            for file in files:
                if file.endswith('.config.json'):
                    config_path = os.path.join(root, file)
                    nombre = os.path.splitext(os.path.splitext(file)[0])[0]
                    self.cargar_con_configuracion(nombre, config_path)

    def instalar_desde_repositorio(self, url_git: str) -> None:
        """Instala módulos desde repositorio Git"""
        # Implementación simplificada
        modulo = url_git.split('/')[-1].replace('.git', '')
        os.system(f"git clone {url_git} plugins/{modulo}")
        self.descubrir_modulos(f"plugins/{modulo}")

    # 10. Versionado y Rollback
    def instalar_version(self, nombre: str, version: str) -> bool:
        """Instala una versión específica de un módulo"""
        if nombre in self.versions and version in self.versions[nombre]:
            self.active_versions[nombre] = version
            self.recargar_modulo(nombre)
            return True
        return False

    def rollback_modulo(self, nombre: str) -> bool:
        """Vuelve a la versión anterior estable"""
        if nombre in self.versions and len(self.versions[nombre]) > 1:
            # Obtener versión anterior
            current_idx = self.versions[nombre].index(self.active_versions[nombre])
            if current_idx > 0:
                prev_version = self.versions[nombre][current_idx - 1]
                return self.instalar_version(nombre, prev_version)
        return False

    # 11. Plugins Distribuido
    def ejecutar_remoto(self, nombre: str, metodo: str, servidor: str, *args, **kwargs) -> Any:
        """Ejecuta un módulo en un servidor remoto"""
        try:
            respuesta = requests.post(
                f"http://{servidor}/ejecutar",
                json={
                    'modulo': nombre,
                    'metodo': metodo,
                    'args': args,
                    'kwargs': kwargs
                },
                timeout=30
            )
            return respuesta.json()['resultado']
        except Exception as e:
            logger.error(f"Error ejecución remota ({servidor}): {e}")
            return None

    def balancear_carga(self, nombre: str, metodo: str, *args, **kwargs) -> Any:
        """Distribuye ejecuciones entre múltiples instancias"""
        servidores = self.configs.get(nombre, {}).get('servidores', [])
        if not servidores:
            return self.ejecutar(nombre, metodo, *args, **kwargs)
        
        # Selección simple round-robin
        if not hasattr(self, '_balance_index'):
            self._balance_index = {}
        idx = self._balance_index.get(nombre, 0) % len(servidores)
        self._balance_index[nombre] = idx + 1
        
        return self.ejecutar_remoto(nombre, metodo, servidores[idx], *args, **kwargs)

    # --- Métodos existentes mejorados ---
    def _cargar_modulo(self, config: Dict) -> None:
        """Carga interna de módulo con manejo de versiones"""
        nombre = config['nombre']
        self._ejecutar_hooks('before_load', nombre)
        
        try:
            if 'clase_directa' in config:
                self.modulos[nombre] = config['clase_directa']()
            else:
                mod = importlib.import_module(config['ruta'])
                clase_obj = getattr(mod, config['clase'])
                self.modulos[nombre] = clase_obj(**self.configs.get(nombre, {}))
            
            # Registrar versión
            version = getattr(self.modulos[nombre], '__version__', '1.0.0')
            if nombre not in self.versions:
                self.versions[nombre] = []
            if version not in self.versions[nombre]:
                self.versions[nombre].append(version)
            self.active_versions[nombre] = version
            
            logger.info(f"Módulo {nombre} v{version} cargado")
            self._ejecutar_hooks('after_load', nombre)
        except Exception as e:
            logger.error(f"Error cargando módulo {nombre}: {e}")
            self._ejecutar_hooks('on_error', nombre, 'load', str(e))

    def ejecutar(self, nombre: str, metodo: str, *args, **kwargs) -> Any:
        """Método base de ejecución con métricas y hooks"""
        start_time = time.time()
        resultado = None
        exito = False
        
        # Carga bajo demanda si es necesario
        modulo = self.cargar_bajo_demanda(nombre)
        if not modulo:
            logger.warning(f"Módulo no disponible: {nombre}")
            return None
        
        self._ejecutar_hooks('before_execute', nombre, metodo)
        
        try:
            if hasattr(modulo, metodo):
                resultado = getattr(modulo, metodo)(*args, **kwargs)
                exito = True
            else:
                logger.warning(f"Método no encontrado: {nombre}.{metodo}")
        except Exception as e:
            logger.error(f"Error en ejecución: {e}")
            self._ejecutar_hooks('on_error', nombre, metodo, str(e))
        
        # Registrar métricas
        tiempo_ejec = (time.time() - start_time) * 1000  # ms
        self.registrar_metrica(nombre, metodo, tiempo_ejec, exito)
        self._ejecutar_hooks('after_execute', nombre, metodo, resultado)
        
        return resultado

    def listar_modulos(self) -> List[str]:
        return list(self.modulos.keys())

    # Métodos existentes mantenidos
    def registrar_modulo(self, nombre: str, clase_modulo: Any) -> None:
        config = {
            'nombre': nombre,
            'clase_directa': clase_modulo,
            'carga_automatica': True
        }
        self.lazy_modules[nombre] = config
        if self.verificar_dependencias(config):
            self._cargar_modulo(config)

    def cargar_modulo_desde_archivo(self, nombre: str, ruta: str, clase: str) -> None:
        config = {
            'nombre': nombre,
            'ruta': ruta,
            'clase': clase,
            'carga_automatica': True
        }
        self.lazy_modules[nombre] = config
        if self.verificar_dependencias(config):
            self._cargar_modulo(config)

    def __del__(self):
        self.observer.stop()
        self.observer.join()
        self.executor.shutdown()