export default function DataExplorer() {
    const collections = [
        { name: "users", docs: 15420, size: "4.2 MB" },
        { name: "orders", docs: 85002, size: "12.8 MB" },
        { name: "products", docs: 420, size: "1.1 MB" },
        { name: "logs", docs: 1200000, size: "450 MB" },
    ];

    return (
        <div>
            <div className="flex justify-between items-center mb-8">
                <h2 className="text-3xl font-bold">Data Explorer</h2>
                <button className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors">
                    Create Collection
                </button>
            </div>

            <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
                <table className="w-full text-left">
                    <thead>
                        <tr className="border-b border-gray-800 bg-gray-900/50">
                            <th className="px-6 py-4 font-medium text-gray-400">Collection Name</th>
                            <th className="px-6 py-4 font-medium text-gray-400">Documents</th>
                            <th className="px-6 py-4 font-medium text-gray-400">Size</th>
                            <th className="px-6 py-4 font-medium text-gray-400 w-24">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-800">
                        {collections.map((col) => (
                            <tr key={col.name} className="hover:bg-gray-800/50 transition-colors cursor-pointer">
                                <td className="px-6 py-4 font-medium text-white">{col.name}</td>
                                <td className="px-6 py-4 text-gray-300">{col.docs.toLocaleString()}</td>
                                <td className="px-6 py-4 text-gray-300">{col.size}</td>
                                <td className="px-6 py-4 text-right">
                                    <button className="text-gray-500 hover:text-white">
                                        Browse
                                    </button>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
