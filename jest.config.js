module.exports = {
  // Automatically clear mock calls, instances, contexts and results before every test
  clearMocks: true,
  // Indicates whether the coverage information should be collected while executing the test
  collectCoverage: true,
  // An array of glob patterns indicating a set of files for which coverage information should be collected
  collectCoverageFrom: [
    'src/**/*.{js,ts}',
    '!**/{node_modules,vendor}/**',
    '!**/{__tests__,__mocks__,__fixtures__}/**',
  ],
  // Indicates which provider should be used to instrument code for coverage
  coverageProvider: 'babel',
  // A list of reporter names that Jest uses when writing coverage reports
  coverageReporters: ['text', 'html'],
  detectOpenHandles: true,
  // Make calling deprecated APIs throw helpful error messages
  errorOnDeprecated: true,
  // A list of paths to directories that Jest should use to search for files in
  roots: ['<rootDir>'],
  // The glob patterns Jest uses to detect test files
  testMatch: ['**/__tests__/**/*.+(ts|tsx|js)', '**/?(*.)+(spec|test).+(ts|tsx|js)'],
  // A map from regular expressions to paths to transformers
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest'],
  },
  // The test environment that will be used for testing
  testEnvironment: 'jest-environment-node',
  verbose: true,
};
