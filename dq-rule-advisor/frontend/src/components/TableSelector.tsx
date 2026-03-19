import { useEffect, useState } from "react";
import { Database, Table, Columns, ChevronRight } from "lucide-react";
import { useStore } from "../store";

export default function TableSelector() {
  const {
    catalog, schema, table, columns,
    setCatalog, setSchema, setTable, setColumns, setStep,
  } = useStore();

  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);

  useEffect(() => {
    fetch("/api/catalog/catalogs")
      .then((r) => r.json())
      .then(setCatalogs);
  }, []);

  useEffect(() => {
    if (!catalog) return;
    fetch(`/api/catalog/schemas/${catalog}`)
      .then((r) => r.json())
      .then(setSchemas);
  }, [catalog]);

  useEffect(() => {
    if (!catalog || !schema) return;
    fetch(`/api/catalog/tables/${catalog}/${schema}`)
      .then((r) => r.json())
      .then(setTables);
  }, [catalog, schema]);

  useEffect(() => {
    if (!catalog || !schema || !table) return;
    fetch(`/api/catalog/columns/${catalog}/${schema}/${table}`)
      .then((r) => r.json())
      .then(setColumns);
  }, [catalog, schema, table]);

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-lg font-semibold text-gray-800">
        <Database className="w-5 h-5 text-blue-600" />
        Select a Table
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Catalog picker */}
        <div>
          <label className="block text-sm font-medium text-gray-600 mb-1">Catalog</label>
          <select
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            value={catalog}
            onChange={(e) => setCatalog(e.target.value)}
          >
            <option value="">-- Select catalog --</option>
            {catalogs.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>

        {/* Schema picker */}
        <div>
          <label className="block text-sm font-medium text-gray-600 mb-1">Schema</label>
          <select
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            disabled={!catalog}
          >
            <option value="">-- Select schema --</option>
            {schemas.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>

        {/* Table picker */}
        <div>
          <label className="block text-sm font-medium text-gray-600 mb-1">Table</label>
          <select
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            value={table}
            onChange={(e) => setTable(e.target.value)}
            disabled={!schema}
          >
            <option value="">-- Select table --</option>
            {tables.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Column preview */}
      {columns.length > 0 && (
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-center gap-2 text-sm font-medium text-gray-600 mb-3">
            <Columns className="w-4 h-4" />
            Table Schema — {columns.length} columns
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {columns.map((c) => (
              <div
                key={c.name}
                className="flex items-center gap-2 text-sm bg-white rounded px-3 py-1.5 border"
              >
                <span className="font-mono text-gray-800">{c.name}</span>
                <span className="text-xs text-gray-400">{c.type}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {table && columns.length > 0 && (
        <button
          onClick={() => setStep("define")}
          className="flex items-center gap-2 bg-blue-600 text-white px-6 py-2.5 rounded-lg hover:bg-blue-700 transition font-medium"
        >
          Continue
          <ChevronRight className="w-4 h-4" />
        </button>
      )}
    </div>
  );
}
