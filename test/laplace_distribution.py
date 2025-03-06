import numpy as np
import matplotlib.pyplot as plt

# Parameters for the Laplace distribution
loc = 0.0  # Mean (mu)
scale = 1.0  # Scale (b)

# Number of data points to generate
size = 1000

# Generate data with Laplace distribution
data = np.random.laplace(loc, scale, size)

# Plot the histogram of the generated data
plt.hist(data, bins=50, density=True, alpha=0.6, color='g')

# Plot the theoretical probability density function (PDF)
x = np.linspace(min(data), max(data), 1000)
pdf = (1 / (2 * scale)) * np.exp(-np.abs(x - loc) / scale)
plt.plot(x, pdf, 'r', linewidth=2)

# Add labels and title
plt.title('Laplace Distribution')
plt.xlabel('Value')
plt.ylabel('Density')

# Show the plot
plt.show()