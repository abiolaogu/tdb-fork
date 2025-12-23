'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import 'graphiql/graphiql.css';

const GraphiQL = dynamic(() => import('graphiql').then((mod) => mod.GraphiQL), {
    ssr: false,
    loading: () => <p>Loading GraphiQL...</p>,
});

export default function GraphiQLPage() {
    const fetcher = async (graphQLParams: any) => {
        const data = await fetch('/api/graphql', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(graphQLParams),
        });
        return data.json();
    };

    return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
            <header className="bg-gray-900 text-white p-4">
                <h1 className="text-xl font-bold">LumaDB Console</h1>
            </header>
            <div style={{ flex: 1 }}>
                <GraphiQL fetcher={fetcher} />
            </div>
        </div>
    );
}
