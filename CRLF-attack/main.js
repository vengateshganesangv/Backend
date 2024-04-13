//CRLF (Carriage Return Line Feed) Attacker Hit
//http://localhost:3000/download?filename=maliciousFile.txt%0D%0AContent-Type:%20text/html%0D%0A%0D%0A%3Cscript%3Ewindow.location%20=%20'http://malicious-site.com'%3C/script%3E
//%0D%0A is the URL-encoded form of \r\n,
//This tells the client's browser that the content being served should be downloaded by the user, and the browser's download manager should use "filename.ext" as the default filename for the saved file

////////////////////////ATTACKED VERSION////////////////////
// const express = require('express');
// const app = express();
// const PORT = 3000;

// app.get('/download', (req, res) => {
//   let filename = req.query.filename || 'defaultName.txt';

//   // Vulnerable to CRLF Injection: User input is directly used in headers without sanitization.
//   res.set('Content-Disposition', `attachment; filename="${filename}"`);

//   res.send('File content would be here.');
// });

// app.listen(PORT, () => {
//   console.log(`Server running on port ${PORT}`);
// });

////////////////////////////////////////////////////////

const express = require("express");
const app = express();
const PORT = 3000;

app.get("/download", (req, res) => {
  let filename = req.query.filename || "defaultName.txt";

  // Basic sanitization to remove CR and LF characters to prevent CRLF Injection
  filename = filename.replace(/[\r\n]/g, "");

  res.set("Content-Disposition", `attachment; filename="${filename}"`);

  res.send("File content would be here.");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
