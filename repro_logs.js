import fs from 'fs';

try {
    if (!fs.existsSync('sync.log')) {
        console.log('sync.log not found');
        process.exit(0);
    }
    const logContent = fs.readFileSync('sync.log', 'utf8');
    const logLines = logContent.trim().split('\n').filter(line => line.trim());

    console.log(`Total lines: ${logLines.length}`);

    let logs = logLines.map((line, index) => {
        try {
            return JSON.parse(line);
        } catch (e) {
            console.log(`Line ${index} failed to parse: ${line.substring(0, 50)}...`);
            return null;
        }
    }).filter(log => log !== null);

    console.log(`Parsed logs: ${logs.length}`);

    if (logs.length > 0) {
        console.log('Sample log:', logs[0]);

        // Test sorting
        try {
            logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            console.log('Sorting successful');
        } catch (e) {
            console.error('Sorting failed:', e);
        }
    }

} catch (error) {
    console.error('Error:', error);
}
