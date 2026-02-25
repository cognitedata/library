import { ClassicAnalysis } from "./components/ClassicAnalysis";

function App() {
  return (
    <main>
      <header style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ fontSize: "1.5rem", fontWeight: 600 }}>
          Classic CDF model analysis
        </h1>
        <p style={{ color: "#94a3b8", marginTop: "0.25rem" }}>
          Understand metadata field distribution across assets, time series,
          events, and sequences. Choose resource type and filter key, then run
          analysis. Results are sorted by count (descending).
        </p>
      </header>
      <ClassicAnalysis />
    </main>
  );
}

export default App;
