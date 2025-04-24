import { MetaProvider, Title } from "@solidjs/meta";
import { Router } from "@solidjs/router";
import { FileRoutes } from "@solidjs/start/router";
import { Suspense } from "solid-js";
import { QueryClient, QueryClientProvider } from "@tanstack/solid-query";

import "./app.css";

// --- Create a QueryClient instance ---
const queryClient = new QueryClient({
});

export default function App() {
  return (
    <Router
      root={(props) => (
        <MetaProvider>
          <Title>SolidStart App</Title>
          {/* Wrap the main content area with QueryClientProvider */}
          <QueryClientProvider client={queryClient}>
            <Suspense fallback={<div>Loading...</div>}>{props.children}</Suspense>
          </QueryClientProvider>
        </MetaProvider>
      )}
    >
      <FileRoutes />
    </Router>
  );
}