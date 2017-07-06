ckptman
=======
 
ckptman is a tool that automates checkpointing in the jobs running on spot instances in order to save as much as job process and money as possible. From now, it implements two checkpointing algorithms:
 
- **Hourly Checkpointing algorithm**: checkpoints are taken just prior to the beginning of next instance hour. Since Amazon is not charging any partial hour, this scheme will save as much tasks as the user is paying.

- **Threshold Checkpointing algorithm**: checkpoints are taken when a rising in the price of the spot instance is observed inside an interval. The interval is a % of the price the user has determined. 

It is created to work with `Amazon Spot Instances`_ , `BLCR`_ checkpointing tool and `SLURM`_ as local resource management system, inside the `EC3`_ cluster deployment tool.

Usage
=====

First, configure the algorithm that you want to apply for the checkpointing (HOUR or THRESHOLD). You can configure also the parameters associated with the algorithm you have chosen and the log file of the tool. For that, you only need to edit the config.py file with your favourite editor::

    vi config.py


Start ckptman just by by typing::

    ./ckptman start


.. _`SLURM`: http://slurm.schedmd.com/
.. _`Amazon Spot Instances`: http://aws.amazon.com/es/ec2/purchasing-options/spot-instances/
.. _`IM`: https://github.com/grycap/im
.. _`EC3`: https://github.com/grycap/ec3
.. _`Pyslurm`: http://www.gingergeeks.co.uk/pyslurm/
.. _`BLCR`: http://crd.lbl.gov/departments/computer-science/CLaSS/research/BLCR/


