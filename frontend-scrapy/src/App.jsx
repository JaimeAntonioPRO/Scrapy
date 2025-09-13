import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

// La dirección donde se está ejecutando tu API de Flask
const API_URL = 'http://localhost:5000';

function App() {
  // --- ESTADOS ---
  const [productos, setProductos] = useState([]);
  const [mensaje, setMensaje] = useState('');
  const [cargando, setCargando] = useState(true);
  const [query, setQuery] = useState(''); // Para guardar el nombre del producto
  const [maxProducts, setMaxProducts] = useState(50); // Valor por defecto

  // --- FUNCIONES ---

  // Obtiene los productos desde el API
  const fetchProductos = async () => {
    setCargando(true);
    try {
      const response = await axios.get(`${API_URL}/api/productos`);
      setProductos(response.data);
    } catch (error) {
      console.error("Error al cargar los productos:", error);
      setMensaje("Error al cargar los productos. ¿Está el API corriendo?");
    }
    setCargando(false);
  };

  // Envía la orden de iniciar un spider
  const handleIniciarSpider = async (nombreSpider) => {
    if (!query.trim()) {
      setMensaje('Por favor, ingresa un producto para buscar.');
      return;
    }

    setMensaje(`Iniciando el spider ${nombreSpider} para buscar "${query}"...`);
    try {
      const response = await axios.post(`${API_URL}/api/iniciar-spider`, { 
        spider: nombreSpider,
        query: query,
        max_products: maxProducts
      });
      setMensaje(response.data.mensaje);

      // Después de un tiempo, actualiza la tabla automáticamente
      setTimeout(() => {
        setMensaje('Actualizando lista de productos...');
        fetchProductos();
      }, 30000); // Aumentado a 30 segundos para dar más tiempo al spider

    } catch (error) {
      console.error("Error al iniciar el spider:", error);
      setMensaje(`Error al iniciar el spider ${nombreSpider}.`);
    }
  };

  // Se ejecuta una vez al cargar el componente para pedir los datos iniciales
  useEffect(() => {
    fetchProductos();
  }, []);

  // --- RENDERIZADO (Lo que se ve en pantalla) ---
  return (
    <div className="App">
      <header className="App-header">
        <h1>Dashboard de Scraping</h1>
      </header>
      
      <main>
        <div className="controles">
          <h2>Control de Spiders</h2>

          <div className="form-group">
            <label htmlFor="query">Producto a Buscar:</label>
            <input 
              type="text" 
              id="query"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Ej: Harina de trigo"
            />
          </div>
          <div className="form-group">
            <label htmlFor="max-products">Máximo de Productos:</label>
            <input 
              type="number"
              id="max-products"
              value={maxProducts}
              onChange={(e) => setMaxProducts(Number(e.target.value))}
              min="1"
            />
          </div>

          <div className="botones-spider">
            <button onClick={() => handleIniciarSpider('heb')}>Buscar en HEB</button>
            <button onClick={() => handleIniciarSpider('soriana')}>Buscar en Soriana</button>
          </div>
          {mensaje && <p className="mensaje-api">{mensaje}</p>}
        </div>

        <div className="tabla-container">
            <h2>Productos en la Base de Datos</h2>
            <button onClick={fetchProductos} className="btn-refrescar">Refrescar Lista 🔄</button>
            
            {cargando ? (
              <p>Cargando productos...</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Imagen</th>
                    <th>Título</th>
                    <th>Precio</th>
                    <th>Tienda</th>
                    <th>Fecha de Captura</th>
                  </tr>
                </thead>
                <tbody>
                  {productos.map(producto => (
                    <tr key={producto.Id}>
                      <td>
                        <img src={producto.UrlImagen} alt={producto.Titulo} className="producto-imagen" />
                      </td>
                      <td className="producto-titulo">{producto.Titulo}</td>
                      <td className="producto-precio">${producto.Precio ? parseFloat(producto.Precio).toFixed(2) : 'N/A'}</td>
                      <td>{producto.Tienda}</td>
                      <td>{new Date(producto.FechaScraping).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
        </div>
      </main>
    </div>
  );
}

export default App;