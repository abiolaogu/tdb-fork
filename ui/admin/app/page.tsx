'use client';

import { useEffect, useState } from 'react';

export default function Home() {
  const [stats, setStats] = useState({
    collections: "12",
    documents: "1.2M",
    events_processed: "85k",
    read_latency: "0.8ms",
  });

  useEffect(() => {
    // Fetch stats from backend
    // Assume proxy or CORS setup allow this
    fetch('http://localhost:8080/api/v1/stats')
      .then(res => res.json())
      .then(data => {
        setStats({
          collections: String(data.collections),
          documents: String(data.documents), // Format K/M
          events_processed: String(data.events_fired),
          read_latency: data.latency_p99
        });
      })
      .catch(err => console.error("Failed to fetch stats:", err));
  }, []);

  const statCards = [
    { title: "Total Collections", value: stats.collections, change: "+2", color: "bg-blue-500" },
    { title: "Documents", value: stats.documents, change: "+15%", color: "bg-purple-500" },
    { title: "Events Processed", value: stats.events_processed, change: "+5.2k", color: "bg-green-500" },
    { title: "Read Latency (p99)", value: stats.read_latency, change: "-0.1ms", color: "bg-orange-500" },
  ];

  return (
    <div>
      <h2 className="text-3xl font-bold mb-8">Dashboard</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {statCards.map((stat) => (
          <div key={stat.title} className="bg-gray-900 p-6 rounded-xl border border-gray-800">
            <div className="flex justify-between items-start mb-4">
              <h3 className="text-gray-400 text-sm font-medium">{stat.title}</h3>
              <div className={`w-2 h-2 rounded-full ${stat.color}`}></div>
            </div>
            <div className="flex items-baseline space-x-2">
              <span className="text-3xl font-bold">{stat.value}</span>
              <span className="text-xs text-green-400 font-medium">{stat.change}</span>
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-gray-900 p-6 rounded-xl border border-gray-800 h-64">
          <h3 className="text-lg font-semibold mb-4">Query Throughput</h3>
          <div className="flex items-center justify-center h-full pb-8 text-gray-500">
            [Chart Placeholder: Recharts or similar]
          </div>
        </div>
        <div className="bg-gray-900 p-6 rounded-xl border border-gray-800 h-64">
          <h3 className="text-lg font-semibold mb-4">System Health</h3>
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span>Node 1 (Leader)</span>
              <span className="text-green-500">Active</span>
            </div>
            <div className="flex justify-between items-center">
              <span>Node 2 (Replica)</span>
              <span className="text-green-500">Active</span>
            </div>
            <div className="flex justify-between items-center">
              <span>Node 3 (Replica)</span>
              <span className="text-green-500">Active</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
