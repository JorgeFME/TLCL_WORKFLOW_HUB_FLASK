"""
Utilidad común para ejecutar SQL desde archivos o sentencias inline.
Provee ejecución secuencial, control de transacciones y limpieza básica de SQL.
"""

import os
import re


class SqlRunner:
    """Ejecutor común de SQL para HANA.

    Permite:
    - Ejecutar scripts desde archivos `.sql`.
    - Ejecutar listas de sentencias inline.
    - Controlar el modo de commit (por sentencia o al final).
    - Detener en el primer error o continuar (stop_on_error).
    """

    def __init__(self, hana_connection):
        """Inicializa el runner.

        Args:
            hana_connection: Instancia de `HanaConnection` con `cursor` y `connection`.
        """
        self.hana_connection = hana_connection

    def _clean_sql(self, sql_text: str) -> str:
        """Elimina comentarios y normaliza el SQL.

        - Remueve comentarios de línea que empiezan con `--`.
        - Remueve comentarios de bloque `/* ... */` (no anidados).
        - Mantiene saltos de línea para preservar formato.
        """
        # Remover comentarios de bloque
        without_block = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
        cleaned_lines = []
        for line in without_block.splitlines():
            stripped = line.strip()
            if stripped.startswith('--'):
                continue
            cleaned_lines.append(line)
        return '\n'.join(cleaned_lines)

    def _split_statements(self, sql_text: str):
        """Divide el texto SQL en sentencias por `;`.

        Nota: Asume que no hay `;` dentro de literales ni procedimientos anidados.
        """
        statements = [stmt.strip() for stmt in sql_text.split(';') if stmt.strip()]
        return statements

    def execute_sql_file(self, file_path: str, commit_mode: str = 'end', stop_on_error: bool = True):
        """Ejecuta un archivo `.sql` secuencialmente.

        Args:
            file_path: Ruta al archivo SQL.
            commit_mode: 'end' para commit al final, 'per_statement' para commit tras cada sentencia.
            stop_on_error: True para detener al primer error.

        Returns:
            dict: {success, message, details}
        """
        try:
            if not os.path.isabs(file_path):
                # Resolver relativo al directorio del archivo que llama puede ser complejo;
                # aquí asumimos que el path recibido es absoluto o relativo correcto.
                file_path = os.path.abspath(file_path)

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_sql = f.read()

            cleaned = self._clean_sql(raw_sql)
            statements = self._split_statements(cleaned)

            cursor = self.hana_connection.cursor
            executed = 0
            errors = []

            for idx, stmt in enumerate(statements, start=1):
                try:
                    cursor.execute(stmt)
                    executed += 1
                    if commit_mode == 'per_statement':
                        try:
                            if hasattr(self.hana_connection, 'connection') and hasattr(self.hana_connection.connection, 'commit'):
                                self.hana_connection.connection.commit()
                        except Exception:
                            pass
                except Exception as e:
                    errors.append({
                        'index': idx,
                        'error': str(e)
                    })
                    if stop_on_error:
                        break

            if commit_mode == 'end' and not errors:
                try:
                    if hasattr(self.hana_connection, 'connection') and hasattr(self.hana_connection.connection, 'commit'):
                        self.hana_connection.connection.commit()
                except Exception:
                    pass

            return {
                'success': len(errors) == 0,
                'message': 'Script ejecutado' if len(errors) == 0 else 'Script ejecutado con errores',
                'details': {
                    'file': file_path,
                    'statements_executed': executed,
                    'errors': errors
                }
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al ejecutar archivo SQL: {str(e)}',
                'details': None
            }

    def execute_statements(self, statements, commit_mode: str = 'end', stop_on_error: bool = True):
        """Ejecuta una lista de sentencias SQL inline.

        Cada elemento de `statements` debe ser una cadena SQL completa terminada sin `;`.

        Returns:
            dict: {success, message, details}
        """
        try:
            cursor = self.hana_connection.cursor
            executed = 0
            errors = []

            for idx, stmt in enumerate(statements, start=1):
                try:
                    cursor.execute(stmt)
                    executed += 1
                    if commit_mode == 'per_statement':
                        try:
                            if hasattr(self.hana_connection, 'connection') and hasattr(self.hana_connection.connection, 'commit'):
                                self.hana_connection.connection.commit()
                        except Exception:
                            pass
                except Exception as e:
                    errors.append({
                        'index': idx,
                        'error': str(e)
                    })
                    if stop_on_error:
                        break

            if commit_mode == 'end' and not errors:
                try:
                    if hasattr(self.hana_connection, 'connection') and hasattr(self.hana_connection.connection, 'commit'):
                        self.hana_connection.connection.commit()
                except Exception:
                    pass

            return {
                'success': len(errors) == 0,
                'message': 'Sentencias ejecutadas' if len(errors) == 0 else 'Sentencias ejecutadas con errores',
                'details': {
                    'statements_executed': executed,
                    'errors': errors
                }
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error al ejecutar sentencias: {str(e)}',
                'details': None
            }