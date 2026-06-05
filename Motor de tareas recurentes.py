import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from contextlib import asynccontextmanager
import random
import time

# Configuración básica de logging mensajeo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ComplexApp")
# metolo colores
VERDE = "\033[92m"
ROJO = "\033[91m"
NARANJA = "\033[38;5;166m"  # naranja oscuro https://www.w3schools.com/colors/colors_picker.asp
AMARILLO = "\033[93m"
AZUL = "\033[94m"
VIOLETA = "\033[95m"
RESET = "\033[0m"


# Dominio

@dataclass
class Task:
    id: int
    name: str
    payload: Dict[str, str]
    created_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3


class TaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class TaskResult:
    task_id: int
    status: str
    data: Optional[Dict] = None
    error: Optional[str] = None


class enMemoriaREpositorio:
    def __init__(self) -> None:
        self._tasks: Dict[int, Task] = {}
        self._results: Dict[int, TaskResult] = {}

    def add_task(self, task: Task) -> None:
        logger.info(
            AMARILLO + "Registrando tarea {task.id} ({task.name})" + RESET
        )
        self._tasks[task.id] = task

    def get_pending_tasks(self) -> List[Task]:
        return list(self._tasks.values())

    def save_result(self, result: TaskResult) -> None:
        logger.info(
            VIOLETA + f"Guardando resultado de tarea {result.task_id}: {result.status}" + RESET
        )
        self._results[result.task_id] = result

    def get_result(self, task_id: int) -> Optional[TaskResult]:
        return self._results.get(task_id)

    def all_results(self) -> List[TaskResult]:
        return list(self._results.values())


class ExternoCliente:
    async def process(self, task: Task) -> Dict:
        # Simula latencia de red random
        await asyncio.sleep(random.uniform(0.1, 0.8))

        # simula errores aleatorios probar varias veces
        if random.random() < 0.3:
            raise RuntimeError(f"Error procesando tarea {task.id}")

        # simula respuesta
        return {
            "task_id": task.id,
            "processed_name": task.name.upper(),
            "payload_size": len(task.payload),
        }


# asincrono

@asynccontextmanager
async def app_context():
    logger.info(
        AZUL + "Inicializando contexto de aplicación..." + RESET)
    repo = enMemoriaREpositorio()
    client = ExternoCliente()

    try:
        yield repo, client
    finally:
        logger.info(
            AZUL + "Cerrando contexto de aplicación..." + RESET
        )


class MandarOrdenes:
    def __init__(self, repo: enMemoriaREpositorio, client: ExternoCliente):
        self.repo = repo
        self.client = client

    async def _execute_single_task(self, task: Task) -> TaskResult:
        logger.info(f"Iniciando tarea {task.id} ({task.name})")
        status = TaskStatus.RUNNING

        try:
            response = await self.client.process(task)
            status = TaskStatus.SUCCESS
            logger.info(
                VERDE + f"Tarea {task.id} completada correctamente" + RESET)  # logger.info(f"Tarea {task.id} completada correctamente") comprueba cmabio primero
            return TaskResult(task_id=task.id, status=status, data=response)
        except Exception as e:
            logger.error(f"Tarea {task.id} falló: {e}")
            # logger.error(ROJO + f"Tarea {task.id} falló: {e}" + RESET) rojo por defecto d aigual pensar en borrarlo o usar otro tono

            task.retries += 1
            if task.retries <= task.max_retries:
                logger.warning(
                    NARANJA + f"Reintentando tarea {task.id} (intento {task.retries})" + RESET
                    ##puede aparecer puede que no probar un par de veces
                )
                return await self._execute_single_task(task)
            else:
                status = TaskStatus.FAILED
                return TaskResult(task_id=task.id, status=status, error=str(e))

    async def run_all(self) -> None:
        tasks = self.repo.get_pending_tasks()
        if not tasks:
            logger.warning("No hay tareas pendientes.")
            return

        coros = [self._execute_single_task(t) for t in tasks]
        results = await asyncio.gather(*coros)

        for result in results:
            self.repo.save_result(result)


async def main():
    async with app_context() as (repo, client):
        # Crear algunas tareas de ejemplo
        for i in range(1, 8):
            task = Task(
                id=i,
                name=f"tarea_{i}",
                payload={"foo": "bar", "index": str(i)},
            )
            repo.add_task(task)

        executor = MandarOrdenes(repo, client)
        await executor.run_all()

        # Mostrar resultados finales
        logger.info("Resultados finales:")
        for result in repo.all_results():
            logger.info(
                f"Tarea {result.task_id}: {result.status} "
                f"-> data={result.data}, error={result.error}"
            )


if __name__ == "__main__":
    asyncio.run(main())
