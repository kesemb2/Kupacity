import axios from 'axios';

const API_BASE = process.env.REACT_APP_API_URL !== undefined
  ? process.env.REACT_APP_API_URL
  : 'https://cinema-back-kjkx.onrender.com';

const api = axios.create({
  baseURL: `${API_BASE}/api`,
  timeout: 10000,
});

export const fetchDashboardSummary = () => api.get('/dashboard/summary').then(r => r.data);
export const fetchMovies = () => api.get('/movies').then(r => r.data);
export const fetchMovieDetail = (id) => api.get(`/movies/${id}`).then(r => r.data);
export const fetchCinemas = () => api.get('/cinemas').then(r => r.data);
export const fetchCities = () => api.get('/cities').then(r => r.data);
export const fetchTicketsByDate = (days = 14) => api.get(`/analytics/tickets-by-date?days=${days}`).then(r => r.data);
export const fetchTicketsByBranch = () => api.get('/analytics/tickets-by-branch').then(r => r.data);
export const fetchTopMovies = (limit = 10) => api.get(`/analytics/top-movies?limit=${limit}`).then(r => r.data);
export const fetchOccupancyByFormat = () => api.get('/analytics/occupancy-by-format').then(r => r.data);
export const fetchTicketsByHour = () => api.get('/analytics/tickets-by-hour').then(r => r.data);
export const fetchOccupancyByDayOfWeek = () => api.get('/analytics/occupancy-by-day-of-week').then(r => r.data);
export const fetchMovieTrends = () => api.get('/analytics/movie-trends').then(r => r.data);
export const fetchDeadScreenings = (threshold = 10) => api.get(`/analytics/dead-screenings?threshold=${threshold}`).then(r => r.data);
export const fetchFormatByBranch = () => api.get('/analytics/format-by-branch').then(r => r.data);
export const fetchBranchEfficiency = () => api.get('/analytics/branch-efficiency').then(r => r.data);
export const fetchGenreStats = () => api.get('/analytics/genre-stats').then(r => r.data);
export const fetchScrapeLogs = () => api.get('/scrape-logs').then(r => r.data);
export const triggerScrape = () => api.post('/scrape/trigger').then(r => r.data);

export const checkHealth = () => api.get('/health', { timeout: 15000 })
  .then(() => ({ ok: true }))
  .catch(() => ({ ok: false }));

export const getDebugScreenshotUrl = () => `${API_BASE}/api/debug-screenshot`;
export const getDebugScreenshotTicketsUrl = () => `${API_BASE}/api/debug-screenshot-tickets`;

export const fetchDebugScreenshots = () => api.get('/debug-screenshots').then(r => r.data);
export const getDebugScreenshotFileUrl = (filename) => `${API_BASE}/api/debug-screenshots/${filename}`;
export const clearDebugScreenshots = () => api.delete('/debug-screenshots').then(r => r.data);

export default api;
