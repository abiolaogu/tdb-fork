export default function APIExplorer() {
    return (
        <div className="h-[calc(100vh-8rem)]">
            <h2 className="text-3xl font-bold mb-4">API Explorer</h2>
            <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden h-full">
                <iframe
                    src="http://localhost:8080/graphql"
                    className="w-full h-full border-0"
                    title="GraphiQL Explorer"
                />
            </div>
        </div>
    );
}
